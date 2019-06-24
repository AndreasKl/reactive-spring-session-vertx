package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.reactiverse.pgclient.Tuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
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
import reactor.core.publisher.MonoSink;

/**
 * A {@link ReactiveSessionRepository} that is implemented using vert.x reactive postgres client.
 */
public class ReactivePostgresSessionRepository
    implements ReactiveSessionRepository<PostgresSession> {

  private static final int DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS = 1800;

  public static final String INSERT_STATEMENT =
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
          + "   $1,"
          + "   $2,"
          + "   $3,"
          + "   $4,"
          + "   $5,"
          + "   $6,"
          + "   $7"
          + " );";

  public static final String UPDATE_STATEMENT =
      "UPDATE session "
          + " SET "
          + "   session_id = $2,"
          + "   session_data = $3,"
          + "   last_accessed_time = $4,"
          + "   expiry_time = $5,"
          + "   max_inactive_interval = $6"
          + " WHERE id = $1;";

  public static final String SELECT_STATEMENT =
      "SELECT "
          + " id, session_id, session_data, creation_time,"
          + " last_accessed_time, max_inactive_interval "
          + "FROM session WHERE session_id = $1;";

  private final PgPool pgPool;
  private final SerializationStrategy serializationStrategy;
  private final DeserializationStrategy deserializationStrategy;
  private final Clock clock;
  private Duration maxInactiveInterval = Duration.ofSeconds(DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);

  public ReactivePostgresSessionRepository(
      PgPool pgPool,
      SerializationStrategy serializationStrategy,
      DeserializationStrategy deserializationStrategy,
      Clock clock) {
    this.pgPool = requireNonNull(pgPool, "pgPool must not be null");
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
            pgPool.preparedQuery(
                INSERT_STATEMENT,
                Tuple.of(
                    postgresSession.internalPrimaryKey,
                    postgresSession.getId(),
                    Buffer.buffer(sessionDataAsBytes(postgresSession)),
                    postgresSession.getCreationTime().toEpochMilli(),
                    postgresSession.getLastAccessedTime().toEpochMilli(),
                    postgresSession.getExpiryTime().toEpochMilli(),
                    (int) postgresSession.getMaxInactiveInterval().getSeconds()),
                asyncResult -> handleInsertOrUpdate(asyncResult, sink, postgresSession));
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
            pgPool.preparedQuery(
                UPDATE_STATEMENT,
                Tuple.of(
                    postgresSession.internalPrimaryKey,
                    postgresSession.getId(),
                    Buffer.buffer(sessionDataAsBytes(postgresSession)),
                    postgresSession.getLastAccessedTime().toEpochMilli(),
                    postgresSession.getExpiryTime().toEpochMilli(),
                    (int) postgresSession.getMaxInactiveInterval().getSeconds()),
                asyncResult -> handleInsertOrUpdate(asyncResult, sink, postgresSession));
          } catch (Exception ex) {
            sink.error(ex);
          }
        });
  }

  private void handleInsertOrUpdate(
      AsyncResult<PgRowSet> asyncResult, MonoSink<Void> sink, PostgresSession postgresSession) {
    if (asyncResult.succeeded()) {
      if (asyncResult.result().rowCount() == 1) {
        postgresSession.clearChangeFlags();
        sink.success();
      } else {
        sink.error(new RuntimeException("SQLStatement did not return the expected row count."));
      }
      return;
    }
    sink.error(asyncResult.cause());
  }

  @Override
  public Mono<PostgresSession> findById(String id) {
    return Mono.create(
        sink ->
            pgPool.preparedQuery(
                SELECT_STATEMENT,
                Tuple.of(id),
                asyncResult -> {
                  if (asyncResult.succeeded()) {
                    if (asyncResult.result().rowCount() == 1) {
                      try {
                        Row row = asyncResult.result().iterator().next();
                        PostgresSession session = mapRowToPostgresSession(row);
                        sink.success(session.isExpired() ? null : session);
                      } catch (Exception ex) {
                        sink.error(ex);
                      }
                    } else {
                      sink.error(
                          new RuntimeException(
                              "SQLStatement did not return the expected row count."));
                    }
                    return;
                  }
                  sink.error(asyncResult.cause());
                }));
  }

  private PostgresSession mapRowToPostgresSession(Row row) {
    Map<String, Object> sessionData =
        deserializationStrategy.deserialize(row.getBuffer("session_data").getBytes());
    return new PostgresSession(
        row.getUUID("id"),
        row.getString("session_id"),
        sessionData,
        Instant.ofEpochMilli(row.getLong("creation_time")),
        Instant.ofEpochMilli(row.getLong("last_accessed_time")),
        Duration.ofSeconds(row.getInteger("max_inactive_interval")));
  }

  @Override
  public Mono<Void> deleteById(String id) {
    return Mono.create(
        sink ->
            pgPool.preparedQuery(
                "DELETE FROM session WHERE session_id = $1;",
                Tuple.of(id),
                t -> {
                  if (t.succeeded()) {
                    sink.success();
                    return;
                  }
                  sink.error(t.cause());
                }));
  }

  private byte[] sessionDataAsBytes(PostgresSession postgresSession) {
    return serializationStrategy.serialize(postgresSession.sessionData);
  }

  final class PostgresSession implements Session {

    private final UUID internalPrimaryKey;
    private final Map<String, Object> sessionData;

    private String sessionId;
    private boolean isNew;
    private boolean changed = true;
    private Instant lastAccessedTime;
    private Instant creationTime;
    private Duration maxInactiveInterval;

    /** Generate a new session. */
    PostgresSession() {
      this.internalPrimaryKey = UUID.randomUUID();
      this.sessionId = UUID.randomUUID().toString();
      this.sessionData = new HashMap<>();
      this.creationTime = ReactivePostgresSessionRepository.this.clock.instant();
      this.lastAccessedTime = ReactivePostgresSessionRepository.this.clock.instant();
      this.maxInactiveInterval = ReactivePostgresSessionRepository.this.maxInactiveInterval;
      this.isNew = true;
    }

    /** Load an existing session. */
    PostgresSession(
        UUID internalPrimaryKey,
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
