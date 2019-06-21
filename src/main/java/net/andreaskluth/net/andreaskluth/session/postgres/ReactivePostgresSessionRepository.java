package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.asyncsql.AsyncSQLClient;
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

  private AsyncSQLClient asyncSQLClient;
  private final SerializationStrategy serializationStrategy;
  private final DeserializationStrategy deserializationStrategy;

  public ReactivePostgresSessionRepository(
      AsyncSQLClient asyncSQLClient,
      SerializationStrategy serializationStrategy,
      DeserializationStrategy deserializationStrategy) {
    this.asyncSQLClient = requireNonNull(asyncSQLClient, "asyncSQLClient must not be null");
    this.serializationStrategy =
        requireNonNull(serializationStrategy, "serializationStrategy must not be null");
    this.deserializationStrategy =
        requireNonNull(deserializationStrategy, "deserializationStrategy must not be null");
  }

  @Override
  public Mono<PostgresSession> createSession() {
    return Mono.defer(() -> Mono.just(new PostgresSession()));
  }

  @Override
  public Mono<Void> save(PostgresSession postgresSession) {
    byte[] sessionData = serializationStrategy.serialize(postgresSession.sessionData);

    return Mono.create(
        sink ->
            asyncSQLClient.updateWithParams(
                "INSERT INTO session (id, session_data) VALUES (?, ?);",
                new JsonArray().add(postgresSession.getId()).add(sessionData),
                t -> {
                  if (t.failed()) {
                    sink.error(t.cause());
                    return;
                  }
                  sink.success(null);
                }));
  }

  @Override
  public Mono<PostgresSession> findById(String id) {
    return Mono.create(
        sink ->
            asyncSQLClient.querySingleWithParams(
                "SELECT id, session_data FROM session WHERE id = ?;",
                new JsonArray().add(id),
                t -> {
                  if (t.failed()) {
                    sink.error(t.cause());
                    return;
                  }
                  try {
                    Map<String, Object> sessionData =
                        deserializationStrategy.deserialize(t.result().getBinary(1));
                    sink.success(new PostgresSession(t.result().getString(0), sessionData));
                  } catch (Exception ex) {
                    sink.error(ex);
                    return;
                  }
                }));
  }

  @Override
  public Mono<Void> deleteById(String id) {
    return Mono.create(
        sink ->
            asyncSQLClient.updateWithParams(
                "DELETE FROM session WHERE id = ?;",
                new JsonArray().add(id),
                t -> {
                  if (t.failed()) {
                    sink.error(t.cause());
                    return;
                  }
                  sink.success(null);
                }));
  }

  final class PostgresSession implements Session {

    private final String id;
    private final boolean isNew;
    private final Map<String, Object> sessionData;

    /** Generate a new session. */
    PostgresSession() {
      this.id = UUID.randomUUID().toString();
      this.sessionData = new HashMap<>();
      this.isNew = true;
    }

    /** Load an existing session. */
    PostgresSession(String id, Map<String, Object> sessionData) {
      this.id = id;
      this.sessionData = sessionData;
      this.isNew = false;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String changeSessionId() {
      return null;
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
      sessionData.put(key, value);
    }

    @Override
    public void removeAttribute(String key) {
      sessionData.remove(key);
    }

    @Override
    public Instant getCreationTime() {
      return null;
    }

    @Override
    public void setLastAccessedTime(Instant instant) {}

    @Override
    public Instant getLastAccessedTime() {
      return null;
    }

    @Override
    public void setMaxInactiveInterval(Duration duration) {}

    @Override
    public Duration getMaxInactiveInterval() {
      return null;
    }

    @Override
    public boolean isExpired() {
      return false;
    }
  }
}
