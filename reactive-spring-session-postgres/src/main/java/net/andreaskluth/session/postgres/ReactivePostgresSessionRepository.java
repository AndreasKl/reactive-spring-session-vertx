package net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.DELETE_EXPIRED_SESSIONS_SQL;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.DELETE_FROM_SESSION_SQL;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.INSERT_SQL;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.REDUCED_UPDATE_SQL;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.SELECT_SQL;
import static net.andreaskluth.session.postgres.ReactivePostgresSessionRepositoryQueries.UPDATE_SQL;

import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgResult;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.reactiverse.pgclient.Tuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import net.andreaskluth.session.postgres.serializer.SerializationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.session.Session;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;

/**
 * A {@link ReactiveSessionRepository} that is implemented using vert.x reactive postgres client.
 */
public class ReactivePostgresSessionRepository
    implements ReactiveSessionRepository<PostgresSession> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ReactivePostgresSessionRepository.class);

  private static final String SEQUENCE_DEFAULT_NAME = "ReactivePostgresSessionRepository";

  private final PgPool pgPool;
  private final SerializationStrategy serializationStrategy;
  private final Clock clock;

  private boolean enableMetrics = false;
  private String sequenceName = SEQUENCE_DEFAULT_NAME;
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
   * Activates {@link Mono#metrics()} for all operations.
   *
   * @param enableMetrics whether metrics should be generated or not.
   */
  public void withMetrics(boolean enableMetrics) {
    this.enableMetrics = enableMetrics;
  }

  /**
   * Set a custom sequence name used for metrics see {@link Mono#name(String)}.
   *
   * @param sequenceName overrides the default sequence name
   */
  public void setSequenceName(String sequenceName) {
    this.sequenceName = sequenceName;
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
    return Mono.defer(() -> Mono.just(new PostgresSession(clock, defaultMaxInactiveInterval)))
        .as(new AddMetricsIfEnabled<>("createSession"));
  }

  @Override
  public Mono<Void> save(PostgresSession postgresSession) {
    requireNonNull(postgresSession, "postgresSession must not be null");

    return Mono.defer(
            () ->
                postgresSession.isNew
                    ? insertSession(postgresSession)
                    : updateSession(postgresSession))
        .as(new AddMetricsIfEnabled<>("save"));
  }

  private Mono<Void> insertSession(PostgresSession postgresSession) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Insert new session with id: {}", postgresSession.sessionId);
    }
    return insertSessionCore(postgresSession)
        .handle((rows, sink) -> handleInsertOrUpdate(postgresSession, rows, sink))
        .then();
  }

  private Mono<Void> updateSession(PostgresSession postgresSession) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Update changed session with id: {} updates session data: {}",
          postgresSession.sessionId,
          postgresSession.changed);
    }
    return updateSessionCore(postgresSession)
        .handle((rows, sink) -> handleInsertOrUpdate(postgresSession, rows, sink))
        .then();
  }

  private Mono<PgRowSet> insertSessionCore(PostgresSession postgresSession) {
    return preparedQuery(INSERT_SQL, buildParametersForInsert(postgresSession));
  }

  private Mono<PgRowSet> updateSessionCore(PostgresSession postgresSession) {
    if (postgresSession.isChanged()) {
      return preparedQuery(UPDATE_SQL, buildParametersForUpdate(postgresSession));
    }
    return preparedQuery(REDUCED_UPDATE_SQL, buildReducedParametersForUpdate(postgresSession));
  }

  private void handleInsertOrUpdate(
      PostgresSession session, PgRowSet pgRowSet, SynchronousSink<Object> sink) {
    if (pgRowSet.rowCount() == 1) {
      session.clearChangeFlags();
      sink.complete();
      return;
    }
    var ex =
        new ReactivePostgresSessionException(
            "SQLStatement did not return the expected row count of 1, did return "
                + pgRowSet.rowCount()
                + " inserted/updated records.");
    sink.error(ex);
  }

  @Override
  public Mono<PostgresSession> findById(String id) {
    requireNonNull(id, "id must not be null");

    return preparedQuery(SELECT_SQL, Tuple.of(id))
        .flatMap(pgRowSet -> Mono.justOrEmpty(mapRowSetToPostgresSession(pgRowSet)))
        .filter(postgresSession -> !postgresSession.isExpired())
        .as(new AddMetricsIfEnabled<>("findById"));
  }

  private PostgresSession mapRowSetToPostgresSession(PgRowSet pgRowSet) {
    return pgRowSet.rowCount() >= 1 ? mapRowToPostgresSession(pgRowSet.iterator().next()) : null;
  }

  private PostgresSession mapRowToPostgresSession(Row row) {
    Buffer sessionDataBuffer = row.getBuffer("session_data");
    Map<String, Object> sessionData = byteBufferAsSessionData(sessionDataBuffer);

    return new PostgresSession(
        clock,
        row.getUUID("id"),
        row.getString("session_id"),
        sessionData,
        Instant.ofEpochMilli(row.getLong("creation_time")),
        Instant.ofEpochMilli(row.getLong("last_accessed_time")),
        Duration.ofSeconds(row.getInteger("max_inactive_interval")));
  }

  @Override
  public Mono<Void> deleteById(String id) {
    requireNonNull(id, "id must not be null");
    return Mono.defer(() -> preparedQuery(DELETE_FROM_SESSION_SQL, Tuple.of(id)).then())
        .as(new AddMetricsIfEnabled<>("deleteById"));
  }

  public Mono<Integer> cleanupExpiredSessions() {
    return Mono.defer(
            () ->
                preparedQuery(DELETE_EXPIRED_SESSIONS_SQL, Tuple.of(clock.millis()))
                    .map(PgResult::rowCount))
        .as(new AddMetricsIfEnabled<>("cleanupExpiredSessions"));
  }

  private Mono<PgRowSet> preparedQuery(String statement, Tuple parameters) {
    return Mono.create(
        sink -> pgPool.preparedQuery(statement, parameters, new MonoToVertxHandlerAdapter<>(sink)));
  }

  private Tuple buildParametersForInsert(PostgresSession postgresSession) {
    return Tuple.of(
        postgresSession.internalPrimaryKey,
        postgresSession.getId(),
        sessionDataAsByteBuffer(postgresSession.sessionData),
        postgresSession.getCreationTime().toEpochMilli(),
        postgresSession.getLastAccessedTime().toEpochMilli(),
        postgresSession.getExpiryTime().toEpochMilli(),
        (int) postgresSession.getMaxInactiveInterval().getSeconds());
  }

  private Tuple buildParametersForUpdate(PostgresSession postgresSession) {
    return Tuple.of(
        postgresSession.internalPrimaryKey,
        postgresSession.getId(),
        sessionDataAsByteBuffer(postgresSession.sessionData),
        postgresSession.getLastAccessedTime().toEpochMilli(),
        postgresSession.getExpiryTime().toEpochMilli(),
        (int) postgresSession.getMaxInactiveInterval().getSeconds());
  }

  private Tuple buildReducedParametersForUpdate(PostgresSession postgresSession) {
    return Tuple.of(
        postgresSession.internalPrimaryKey,
        postgresSession.getId(),
        postgresSession.getLastAccessedTime().toEpochMilli(),
        postgresSession.getExpiryTime().toEpochMilli(),
        (int) postgresSession.getMaxInactiveInterval().getSeconds());
  }

  private Buffer sessionDataAsByteBuffer(Map<String, Object> sessionData) {
    if (sessionData.isEmpty()) {
      return null;
    }
    return Buffer.buffer(serializationStrategy.serialize(sessionData));
  }

  private Map<String, Object> byteBufferAsSessionData(Buffer sessionDataBuffer) {
    if (sessionDataBuffer == null) {
      return new HashMap<>();
    }
    return serializationStrategy.deserialize(sessionDataBuffer.getBytes());
  }

  static class PostgresSession implements Session {

    private final UUID internalPrimaryKey;
    private final Map<String, Object> sessionData;
    private final Clock clock;

    private String sessionId;
    private boolean isNew;
    private boolean changed = true;
    private Instant lastAccessedTime;
    private Instant creationTime;
    private Duration maxInactiveInterval;

    /** Generate a new session. */
    PostgresSession(Clock clock, Duration maxInactiveInterval) {
      this.clock = clock;
      this.internalPrimaryKey = UUID.randomUUID();
      this.sessionId = UUID.randomUUID().toString();
      this.sessionData = new HashMap<>();
      this.maxInactiveInterval = maxInactiveInterval;
      this.creationTime = clock.instant();
      this.lastAccessedTime = clock.instant();
      this.isNew = true;
    }

    /** Load an existing session. */
    PostgresSession(
        Clock clock,
        UUID internalPrimaryKey,
        String sessionId,
        Map<String, Object> sessionData,
        Instant creationTime,
        Instant lastAccessedTime,
        Duration maxInactiveInterval) {
      this.clock = clock;
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

  private static class MonoToVertxHandlerAdapter<T> implements Handler<AsyncResult<T>> {

    private final MonoSink<T> sink;

    private MonoToVertxHandlerAdapter(MonoSink<T> sink) {
      this.sink = requireNonNull(sink, "sink must not be null");
    }

    @Override
    public void handle(AsyncResult<T> event) {
      if (event.succeeded()) {
        sink.success(event.result());
        return;
      }
      sink.error(event.cause());
    }
  }

  private class AddMetricsIfEnabled<T> implements Function<Mono<T>, Mono<T>> {

    private String methodName;

    private AddMetricsIfEnabled(String methodName) {
      this.methodName = requireNonNull(methodName, "methodName must not be null");
    }

    @Override
    public Mono<T> apply(Mono<T> toDecorateWithMetrics) {
      return enableMetrics
          ? toDecorateWithMetrics.metrics().name(sequenceName).tag("method", methodName)
          : toDecorateWithMetrics;
    }
  }
}
