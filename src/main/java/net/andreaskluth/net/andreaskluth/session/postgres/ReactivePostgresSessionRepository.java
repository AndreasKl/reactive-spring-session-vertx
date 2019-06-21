package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.andreaskluth.net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.session.Session;
import reactor.core.publisher.Mono;

/**
 * A {@link ReactiveSessionRepository} that is implemented using vert.x reactive postgres client.
 */
public class ReactivePostgresSessionRepository
    implements ReactiveSessionRepository<PostgresSession> {

  public static final int DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS = 1800;

  private final AsyncSQLClient asyncSQLClient;
  private final SerializationStrategy serializationStrategy;
  private final DeserializationStrategy deserializationStrategy;
  private final Clock clock;
  private Duration maxInactiveInterval = Duration.ofSeconds(DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);

  public ReactivePostgresSessionRepository(
      AsyncSQLClient asyncSQLClient,
      SerializationStrategy serializationStrategy,
      DeserializationStrategy deserializationStrategy,
      Clock clock) {
    this.asyncSQLClient = requireNonNull(asyncSQLClient, "asyncSQLClient must not be null");
    this.serializationStrategy =
        requireNonNull(serializationStrategy, "serializationStrategy must not be null");
    this.deserializationStrategy =
        requireNonNull(deserializationStrategy, "deserializationStrategy must not be null");
    this.clock = requireNonNull(clock, "clock must not be null");
  }

  @Override
  public Mono<PostgresSession> createSession() {
    return Mono.defer(() -> Mono.just(new PostgresSession()));
  }

  @Override
  public Mono<Void> save(PostgresSession postgresSession) {
    return postgresSession.isNew ? insertSession(postgresSession) : updateSession(postgresSession);
  }

  private Mono<Void> insertSession(PostgresSession postgresSession) {
    return Mono.create(
        sink -> {
          try {
            byte[] sessionData = sessionDataAsBytes(postgresSession);

            asyncSQLClient.updateWithParams(
                "INSERT INTO session "
                    + " ("
                    + "   id,"
                    + "   session_id,"
                    + "   session_data,"
                    + "   creation_time,"
                    + "   last_accessed_time,"
                    + "   expiry_time,"
                    + "   max_inactive_interval"
                    + " ) "
                    + " VALUES "
                    + " ("
                    + "   ?,"
                    + "   ?,"
                    + "   ?,"
                    + "   ?,"
                    + "   ?,"
                    + "   ?,"
                    + "   ?"
                    + " );",
                new JsonArray()
                    .add(postgresSession.internalPrimaryKey)
                    .add(postgresSession.getId())
                    .add(sessionData)
                    .add(postgresSession.getCreationTime().toEpochMilli())
                    .add(postgresSession.getLastAccessedTime().toEpochMilli())
                    .add(postgresSession.getExpiryTime().toEpochMilli())
                    .add((int) postgresSession.getMaxInactiveInterval().getSeconds()),
                t -> {
                  if (t.succeeded() && t.result().getUpdated() == 1) {
                    postgresSession.clearChangeFlags();
                    sink.success(null);
                    return;
                  }
                  sink.error(
                      t.cause() == null
                          ? new RuntimeException("SQLStatement did not succeed.")
                          : t.cause());
                });
          } catch (Exception ex) {
            sink.error(ex);
          }
        });
  }

  private Mono<Void> updateSession(PostgresSession postgresSession) {
    // FIXME: Optimize when no data was changed...
    // Only update the last_accessed_time, expiry_time and max_inactive_interval
    return Mono.create(
        sink -> {
          try {
            byte[] sessionData = sessionDataAsBytes(postgresSession);
            asyncSQLClient.updateWithParams(
                "UPDATE session "
                    + " SET "
                    + "   session_id = ?,"
                    + "   session_data = ?,"
                    + "   last_accessed_time = ?,"
                    + "   expiry_time = ?,"
                    + "   max_inactive_interval = ?"
                    + " WHERE id = ?;",
                new JsonArray()
                    .add(postgresSession.getId())
                    .add(sessionData)
                    .add(postgresSession.getLastAccessedTime().toEpochMilli())
                    .add(postgresSession.getExpiryTime().toEpochMilli())
                    .add((int) postgresSession.getMaxInactiveInterval().getSeconds())
                    .add(postgresSession.internalPrimaryKey),
                t -> {
                  if (t.succeeded() && t.result().getUpdated() == 1) {
                    postgresSession.clearChangeFlags();
                    sink.success(null);
                    return;
                  }
                  sink.error(
                      t.cause() == null
                          ? new RuntimeException("SQLStatement did not succeed.")
                          : t.cause());
                });
          } catch (Exception ex) {
            sink.error(ex);
          }
        });
  }

  @Override
  public Mono<PostgresSession> findById(String id) {
    return Mono.create(
        sink ->
            asyncSQLClient.querySingleWithParams(
                "SELECT "
                    + " id, session_id, session_data, creation_time,"
                    + " last_accessed_time, max_inactive_interval "
                    + "FROM session WHERE session_id = ?;",
                new JsonArray().add(id),
                t -> {
                  if (t.failed()) {
                    sink.error(t.cause());
                    return;
                  }
                  try {
                    JsonArray result = t.result();
                    Map<String, Object> sessionData =
                        deserializationStrategy.deserialize(result.getBinary(2));
                    PostgresSession session =
                        new PostgresSession(
                            result.getString(0),
                            result.getString(1),
                            sessionData,
                            Instant.ofEpochMilli(result.getLong(3)),
                            Instant.ofEpochMilli(result.getLong(4)),
                            Duration.ofSeconds(result.getInteger(5)));
                    sink.success(session.isExpired() ? null : session);
                  } catch (Exception ex) {
                    sink.error(ex);
                  }
                }));
  }

  @Override
  public Mono<Void> deleteById(String id) {
    return Mono.create(
        sink ->
            asyncSQLClient.updateWithParams(
                "DELETE FROM session WHERE session_id = ?;",
                new JsonArray().add(id),
                t -> {
                  if (t.failed()) {
                    sink.error(t.cause());
                    return;
                  }
                  sink.success(null);
                }));
  }

  private byte[] sessionDataAsBytes(PostgresSession postgresSession) {
    return serializationStrategy.serialize(postgresSession.sessionData);
  }

  final class PostgresSession implements Session {

    private final String internalPrimaryKey;
    private final Map<String, Object> sessionData;

    private String sessionId;
    private boolean isNew;
    private boolean changed = true;
    private Instant lastAccessedTime;
    private Instant creationTime;
    private Duration maxInactiveInterval;

    /** Generate a new session. */
    PostgresSession() {
      this.internalPrimaryKey = UUID.randomUUID().toString();
      this.sessionId = UUID.randomUUID().toString();
      this.sessionData = new HashMap<>();
      this.creationTime = ReactivePostgresSessionRepository.this.clock.instant();
      this.lastAccessedTime = ReactivePostgresSessionRepository.this.clock.instant();
      this.maxInactiveInterval = ReactivePostgresSessionRepository.this.maxInactiveInterval;
      this.isNew = true;
    }

    /** Load an existing session. */
    PostgresSession(
        String internalPrimaryKey,
        String sessionId,
        Map<String, Object> sessionData,
        Instant creationTime,
        Instant lastAccessedTime,
        Duration maxInactiveInterval) {
      this.internalPrimaryKey = internalPrimaryKey;
      this.sessionId = sessionId;
      this.sessionData = sessionData;
      this.creationTime = creationTime;
      this.lastAccessedTime = lastAccessedTime;
      this.maxInactiveInterval = maxInactiveInterval;
      this.isNew = false;
    }

    @Override
    public String getId() {
      return sessionId;
    }

    @Override
    public String changeSessionId() {
      changed = true;
      this.sessionId = UUID.randomUUID().toString();
      return this.sessionId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
      return (T) sessionData.get(key);
    }

    @Override
    public Set<String> getAttributeNames() {
      return sessionData.keySet();
    }

    @Override
    public void setAttribute(String key, Object value) {
      changed = true;
      sessionData.put(key, value);
    }

    @Override
    public void removeAttribute(String key) {
      changed = true;
      sessionData.remove(key);
    }

    @Override
    public Instant getCreationTime() {
      return creationTime;
    }

    @Override
    public void setLastAccessedTime(Instant lastAccessedTime) {
      requireNonNull(lastAccessedTime, "lastAccessedTime must not be null");
      this.lastAccessedTime = lastAccessedTime;
    }

    @Override
    public Instant getLastAccessedTime() {
      return lastAccessedTime;
    }

    @Override
    public void setMaxInactiveInterval(Duration maxInactiveInterval) {
      requireNonNull(maxInactiveInterval, "maxInactiveInterval must not be null");
      this.maxInactiveInterval = maxInactiveInterval;
    }

    @Override
    public Duration getMaxInactiveInterval() {
      return maxInactiveInterval;
    }

    @Override
    public boolean isExpired() {
      return isExpired(clock.instant());
    }

    void clearChangeFlags() {
      this.isNew = false;
      this.changed = false;
    }

    boolean isChanged() {
      return changed;
    }

    Instant getExpiryTime() {
      return getLastAccessedTime().plus(getMaxInactiveInterval());
    }

    boolean isExpired(Instant now) {
      if (maxInactiveInterval.isNegative()) {
        return false;
      }
      return now.minus(maxInactiveInterval).compareTo(lastAccessedTime) >= 0;
    }
  }
}
