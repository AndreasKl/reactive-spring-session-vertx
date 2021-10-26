package net.andreaskluth.session.core;

import static java.util.Objects.requireNonNull;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository.ReactiveSession;
import net.andreaskluth.session.core.serializer.DeserializationException;
import net.andreaskluth.session.core.serializer.SerializationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.session.Session;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/** A {@link ReactiveSessionRepository} using vert.x reactive database client. */
public class ReactiveVertxSessionRepository implements ReactiveSessionRepository<ReactiveSession> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ReactiveVertxSessionRepository.class);

  private static final String METRIC_SEQUENCE_DEFAULT_NAME = "ReactiveVertxSessionRepository";

  private final Pool pool;
  private final ReactiveVertxSessionRepositoryQueries repositoryQueries;
  private final SerializationStrategy serializationStrategy;
  private final Clock clock;

  private boolean enableMetrics = false;
  private String metricSequenceName = METRIC_SEQUENCE_DEFAULT_NAME;
  private Duration defaultMaxInactiveInterval = Duration.ofMinutes(30);
  private boolean invalidateSessionOnDeserializationError = false;

  /**
   * Creates a new instance.
   *
   * @param pool the database pool
   * @param repositoryQueries the sql queries to use
   * @param serializationStrategy the {@link SerializationStrategy} to read and write session data *
   *     with
   * @param clock the {@link Clock} to use
   */
  public ReactiveVertxSessionRepository(
      Pool pool,
      ReactiveVertxSessionRepositoryQueries repositoryQueries,
      SerializationStrategy serializationStrategy,
      Clock clock) {
    this.pool = requireNonNull(pool, "pool must not be null");
    this.repositoryQueries =
        requireNonNull(repositoryQueries, "repositoryQueries must not be null");
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
   * Instead of propagating {@link DeserializationException} to the caller, nothing is returned
   * resulting in the creation of a new session. This is useful if a java class stored in the
   * session was updated a serial version id mismatch occurs.
   *
   * @param invalidateSessionOnDeserializationError whether serialization errors should be
   *     propagated to the caller.
   */
  public void setInvalidateSessionOnDeserializationError(
      boolean invalidateSessionOnDeserializationError) {
    this.invalidateSessionOnDeserializationError = invalidateSessionOnDeserializationError;
  }

  /**
   * Set a custom sequence name used for metrics see {@link Mono#name(String)}.
   *
   * @param sequenceName overrides the default sequence name
   */
  public void setMetricSequenceName(String sequenceName) {
    this.metricSequenceName = requireNonNull(sequenceName, "sequenceName must not be null");
  }

  /**
   * Sets the maximum inactive interval before unused sessions are invalidated. A negative time
   * indicates that the session will never timeout. The default is 1800 (30 minutes).
   *
   * @param maxInactiveInterval the {@link Duration} that the {@link Session} should be kept alive
   *     between client requests.
   */
  public void setDefaultMaxInactiveInterval(Duration maxInactiveInterval) {
    this.defaultMaxInactiveInterval =
        requireNonNull(maxInactiveInterval, "maxInactiveInterval must not be null");
  }

  @Override
  public Mono<ReactiveSession> createSession() {
    return Mono.defer(() -> Mono.just(new ReactiveSession(clock, defaultMaxInactiveInterval)))
        .as(addMetricsIfEnabled("createSession"));
  }

  @Override
  public Mono<Void> save(ReactiveSession reactiveSession) {
    requireNonNull(reactiveSession, "reactiveSession must not be null");

    return Mono.defer(
            () ->
                reactiveSession.isNew
                    ? insertSession(reactiveSession)
                    : updateSession(reactiveSession))
        .as(addMetricsIfEnabled("save"));
  }

  private Mono<Void> insertSession(ReactiveSession reactiveSession) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Insert new session with id: {}", reactiveSession.sessionId);
    }
    return insertSessionCore(reactiveSession)
        .handle((rows, sink) -> handleInsert(reactiveSession, rows, sink))
        .then();
  }

  private Mono<Void> updateSession(ReactiveSession reactiveSession) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Update changed session with id: {} updates session data: {}",
          reactiveSession.sessionId,
          reactiveSession.changed);
    }
    return updateSessionCore(reactiveSession)
        .handle((rows, sink) -> handleUpdate(reactiveSession, rows, sink))
        .then();
  }

  private Mono<RowSet<Row>> insertSessionCore(ReactiveSession reactiveSession) {
    return preparedQuery(repositoryQueries.insertSql(), buildParametersForInsert(reactiveSession));
  }

  private Mono<RowSet<Row>> updateSessionCore(ReactiveSession reactiveSession) {
    if (reactiveSession.isChanged()) {
      return preparedQuery(
          repositoryQueries.updateSql(), buildParametersForUpdate(reactiveSession));
    }
    return preparedQuery(
        repositoryQueries.reducedUpdateSql(),
        buildParametersWithoutSessionDataForUpdate(reactiveSession));
  }

  private void handleInsert(
      ReactiveSession session, RowSet<?> rowSet, SynchronousSink<Object> sink) {
    if (rowSet.rowCount() == 1) {
      session.clearChangeFlags();
      sink.complete();
      return;
    }
    ReactiveSessionException ex =
        new ReactiveSessionException(
            "SQL insert statement did not return the expected row count of 1, did return "
                + rowSet.rowCount()
                + " inserted/updated records.");
    sink.error(ex);
  }

  private void handleUpdate(
      ReactiveSession session, RowSet<?> rowSet, SynchronousSink<Object> sink) {
    if (rowSet.rowCount() >= 1) {
      session.clearChangeFlags();
      sink.complete();
      return;
    }
    ReactiveSessionException ex =
        new ReactiveSessionException(
            "SQL update statement did not return the expected row count, did return "
                + rowSet.rowCount()
                + " inserted/updated records.");
    sink.error(ex);
  }

  @Override
  public Mono<ReactiveSession> findById(String id) {
    requireNonNull(id, "id must not be null");

    Mono<ReactiveSession> findById =
        preparedQuery(repositoryQueries.selectSql(), Tuple.of(id))
            .flatMap(rowSet -> Mono.justOrEmpty(mapRowSetToSession(rowSet)))
            .filter(reactiveSession -> !reactiveSession.isExpired())
            .as(addMetricsIfEnabled("findById"));

    if (invalidateSessionOnDeserializationError) {
      return findById.onErrorResume(DeserializationException.class, e -> Mono.empty());
    }
    return findById;
  }

  private ReactiveSession mapRowSetToSession(RowSet<Row> rowSet) {
    return rowSet.size() >= 1 ? mapRowToSession(rowSet.iterator().next()) : null;
  }

  private ReactiveSession mapRowToSession(Row row) {
    Buffer sessionDataBuffer = row.getBuffer("session_data");
    Map<String, Object> sessionData = byteBufferAsSessionData(sessionDataBuffer);

    return new ReactiveSession(
        clock,
        tryToObtainUUID(row, "id"),
        row.getString("session_id"),
        sessionData,
        Instant.ofEpochMilli(row.getLong("creation_time")),
        Instant.ofEpochMilli(row.getLong("last_accessed_time")),
        Duration.ofSeconds(row.getInteger("max_inactive_interval")));
  }

  private UUID tryToObtainUUID(Row row, String fieldName) {
    // FIXME: Until vertx-mysql does support UUIDs this artistic/pragmatic workaround exists.
    try {
      return row.getUUID(fieldName);
    } catch (UnsupportedOperationException e) {
      return UUID.fromString(row.getString(fieldName));
    }
  }

  @Override
  public Mono<Void> deleteById(String id) {
    requireNonNull(id, "id must not be null");
    return Mono.defer(
            () -> preparedQuery(repositoryQueries.deleteFromSessionSql(), Tuple.of(id)).then())
        .as(addMetricsIfEnabled("deleteById"));
  }

  public Mono<Integer> cleanupExpiredSessions() {
    return Mono.defer(
            () -> {
              LOGGER.info("Removing outdated session entries.");
              return preparedQuery(
                      repositoryQueries.deleteExpiredSessionsSql(), Tuple.of(clock.millis()))
                  .map(RowSet::rowCount);
            })
        .as(addMetricsIfEnabled("cleanupExpiredSessions"));
  }

  private <T> Function<Mono<T>, Mono<T>> addMetricsIfEnabled(String methodName) {
    return toDecorateWithMetrics ->
        enableMetrics
            ? toDecorateWithMetrics.name(metricSequenceName).tag("method", methodName).metrics()
            : toDecorateWithMetrics;
  }

  private Mono<RowSet<Row>> preparedQuery(String statement, Tuple parameters) {
    return Mono.create(
        sink ->
            pool.preparedQuery(statement)
                .execute(parameters, new MonoToVertxHandlerAdapter<>(sink)));
  }

  private Tuple buildParametersForInsert(ReactiveSession reactiveSession) {
    return Tuple.of(
        reactiveSession.internalPrimaryKey,
        reactiveSession.getId(),
        sessionDataAsByteBuffer(reactiveSession.sessionData),
        reactiveSession.getCreationTime().toEpochMilli(),
        reactiveSession.getLastAccessedTime().toEpochMilli(),
        reactiveSession.getExpiryTime().toEpochMilli(),
        (int) reactiveSession.getMaxInactiveInterval().getSeconds());
  }

  private Tuple buildParametersForUpdate(ReactiveSession reactiveSession) {
    return Tuple.of(
        reactiveSession.getId(),
        sessionDataAsByteBuffer(reactiveSession.sessionData),
        reactiveSession.getLastAccessedTime().toEpochMilli(),
        reactiveSession.getExpiryTime().toEpochMilli(),
        (int) reactiveSession.getMaxInactiveInterval().getSeconds(),
        reactiveSession.internalPrimaryKey);
  }

  private Tuple buildParametersWithoutSessionDataForUpdate(ReactiveSession reactiveSession) {
    return Tuple.of(
        reactiveSession.getId(),
        reactiveSession.getLastAccessedTime().toEpochMilli(),
        reactiveSession.getExpiryTime().toEpochMilli(),
        (int) reactiveSession.getMaxInactiveInterval().getSeconds(),
        reactiveSession.internalPrimaryKey);
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

  public static class ReactiveSession implements Session {

    private final UUID internalPrimaryKey;
    private final Map<String, Object> sessionData;
    private final Clock clock;

    private String sessionId;
    private boolean isNew;
    private boolean changed = true;
    private final Instant creationTime;
    private Instant lastAccessedTime;
    private Duration maxInactiveInterval;

    /** Generate a new session. */
    ReactiveSession(Clock clock, Duration maxInactiveInterval) {
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
    ReactiveSession(
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

    public void clearChangeFlags() {
      this.isNew = false;
      this.changed = false;
    }

    public boolean isChanged() {
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
