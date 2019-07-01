package net.andreaskluth.session.postgres;

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
import net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import net.andreaskluth.session.postgres.serializer.SerializationStrategy;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.session.Session;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 * A {@link ReactiveSessionRepository} that is implemented using vert.x reactive postgres client.
 */
public class ReactivePostgresSessionRepository
    implements ReactiveSessionRepository<PostgresSession> {

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

  public static final String MINIMAL_UPDATE_STATEMENT =
      "UPDATE session "
          + " SET "
          + "   session_id = $2,"
          + "   last_accessed_time = $3,"
          + "   expiry_time = $4,"
          + "   max_inactive_interval = $5"
          + " WHERE id = $1;";

  public static final String SELECT_STATEMENT =
      "SELECT "
          + " id, session_id, session_data, creation_time,"
          + " last_accessed_time, max_inactive_interval "
          + "FROM session WHERE session_id = $1;";

  public static final String DELETE_FROM_SESSION_QUERY =
      "DELETE FROM session WHERE session_id = $1 RETURNING *;";

  public static final String DELETE_EXPIRED_SESSIONS_QUERY =
      "DELETE FROM session WHERE expiry_time < $1 AND max_inactive_interval >= 0;";

  private final PgPool pgPool;
  private final SerializationStrategy serializationStrategy;
  private final Clock clock;
  private Duration defaultMaxInactiveInterval = Duration.ofSeconds(1800);

  /**
   * Creates a new instance.
   *
   * @param pgPool the database pool
   * @param serializationStrategy the {@link SerializationStrategy} to read and write session data
   *     with.
   * @param clock the {@link Clock} to use
   */
  public ReactivePostgresSessionRepository(
      PgPool pgPool, SerializationStrategy serializationStrategy, Clock clock) {
    this.pgPool = requireNonNull(pgPool, "pgPool must not be null");
    this.serializationStrategy =
        requireNonNull(serializationStrategy, "serializationStrategy must not be null");
    this.clock = requireNonNull(clock, "clock must not be null");
  }

  /**
   * Sets the maximum inactive interval in seconds between requests before newly created sessions
   * will be invalidated. A negative time indicates that the session will never timeout. The default
   * is 1800 (30 minutes).
   *
   * @param defaultMaxInactiveInterval the number of seconds that the {@link Session} should be kept
   *     alive between client requests.
   */
  public void setDefaultMaxInactiveInterval(int defaultMaxInactiveInterval) {
    this.defaultMaxInactiveInterval = Duration.ofSeconds(defaultMaxInactiveInterval);
  }

  @Override
  public Mono<PostgresSession> createSession() {
    return Mono.defer(() -> Mono.just(new PostgresSession()));
  }

  @Override
  public Mono<Void> save(PostgresSession postgresSession) {
    return Mono.defer(
        () ->
            postgresSession.isNew
                ? insertSession(postgresSession)
                : updateSession(postgresSession));
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
                    sessionDataAsByteBuffer(postgresSession),
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
    return Mono.create(
        sink -> {
          if (postgresSession.isChanged()) {
            updateSession(postgresSession, sink);
          } else {
            updateSessionWithoutSessionData(postgresSession, sink);
          }
        });
  }

  private void updateSession(PostgresSession postgresSession, MonoSink<Void> sink) {
    try {
      pgPool.preparedQuery(
          UPDATE_STATEMENT,
          Tuple.of(
              postgresSession.internalPrimaryKey,
              postgresSession.getId(),
              sessionDataAsByteBuffer(postgresSession),
              postgresSession.getLastAccessedTime().toEpochMilli(),
              postgresSession.getExpiryTime().toEpochMilli(),
              (int) postgresSession.getMaxInactiveInterval().getSeconds()),
          asyncResult -> handleInsertOrUpdate(asyncResult, sink, postgresSession));
    } catch (Exception ex) {
      sink.error(ex);
    }
  }

  private void updateSessionWithoutSessionData(
      PostgresSession postgresSession, MonoSink<Void> sink) {
    try {
      pgPool.preparedQuery(
          MINIMAL_UPDATE_STATEMENT,
          Tuple.of(
              postgresSession.internalPrimaryKey,
              postgresSession.getId(),
              postgresSession.getLastAccessedTime().toEpochMilli(),
              postgresSession.getExpiryTime().toEpochMilli(),
              (int) postgresSession.getMaxInactiveInterval().getSeconds()),
          asyncResult -> handleInsertOrUpdate(asyncResult, sink, postgresSession));
    } catch (Exception ex) {
      sink.error(ex);
    }
  }

  private void handleInsertOrUpdate(
      AsyncResult<PgRowSet> asyncResult, MonoSink<Void> sink, PostgresSession postgresSession) {
    if (asyncResult.succeeded()) {
      if (asyncResult.result().rowCount() == 1) {
        postgresSession.clearChangeFlags();
        sink.success();
      } else {
        sink.error(
            new ReactivePostgresSessionException(
                "SQLStatement did not return the expected row count."));
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
                      sink.success(null);
                    }
                    return;
                  }
                  sink.error(asyncResult.cause());
                }));
  }

  private PostgresSession mapRowToPostgresSession(Row row) {
    Buffer sessionDataBuffer = row.getBuffer("session_data");
    Map<String, Object> sessionData =
        sessionDataBuffer == null
            ? new HashMap<>()
            : serializationStrategy.deserialize(sessionDataBuffer.getBytes());

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
    return Mono.defer(
        () ->
            Mono.create(
                sink ->
                    pgPool.preparedQuery(
                        DELETE_FROM_SESSION_QUERY,
                        Tuple.of(id),
                        t -> {
                          if (t.succeeded()) {
                            sink.success();
                            return;
                          }
                          sink.error(t.cause());
                        })));
  }

  public Mono<Integer> cleanupExpiredSessions() {
    return Mono.defer(
        () ->
            Mono.create(
                sink ->
                    pgPool.preparedQuery(
                        DELETE_EXPIRED_SESSIONS_QUERY,
                        Tuple.of(clock.millis()),
                        t -> {
                          if (t.succeeded()) {
                            sink.success(t.result().rowCount());
                            return;
                          }
                          sink.error(t.cause());
                        })));
  }

  private Buffer sessionDataAsByteBuffer(PostgresSession postgresSession) {
    if (postgresSession.sessionData.isEmpty()) {
      return null;
    }
    return Buffer.buffer(serializationStrategy.serialize(postgresSession.sessionData));
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
      this.maxInactiveInterval = ReactivePostgresSessionRepository.this.defaultMaxInactiveInterval;
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
