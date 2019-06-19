package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import java.time.Duration;
import java.time.Instant;
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

  public ReactivePostgresSessionRepository(AsyncSQLClient asyncSQLClient) {
    this.asyncSQLClient = requireNonNull(asyncSQLClient, "asyncSQLClient must not be null");
  }

  @Override
  public Mono<PostgresSession> createSession() {
    return Mono.defer(() -> Mono.just(new PostgresSession()));
  }

  @Override
  public Mono<Void> save(PostgresSession postgresSession) {
    return Mono.create(
        sink ->
            asyncSQLClient.updateWithParams(
                "INSERT INTO session (id) VALUES (?);",
                new JsonArray().add(postgresSession.getId()),
                t -> {
                  if (t.failed()) {
                    sink.error(new RuntimeException());
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
                "SELECT id FROM session WHERE id = ?;",
                new JsonArray().add(id),
                t -> {
                  if (t.failed()) {
                    sink.error(new RuntimeException());
                    return;
                  }
                  sink.success(new PostgresSession(t.result().getString(0)));
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
                    sink.error(new RuntimeException());
                    return;
                  }
                  sink.success(null);
                }));
  }

  final class PostgresSession implements Session {

    private final String id;
    private final boolean isNew;

    /** Generate a new session. */
    PostgresSession() {
      this.id = UUID.randomUUID().toString();
      this.isNew = true;
    }

    /**
     * Load an existing session.
     *
     * @param id
     */
    PostgresSession(String id) {
      this.id = id;
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
    public <T> T getAttribute(String s) {
      return null;
    }

    @Override
    public Set<String> getAttributeNames() {
      return null;
    }

    @Override
    public void setAttribute(String s, Object o) {}

    @Override
    public void removeAttribute(String s) {}

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
