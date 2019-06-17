package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.vertx.ext.asyncsql.AsyncSQLClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import net.andreaskluth.net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.session.Session;
import reactor.core.CoreSubscriber;
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
        sink -> {
          asyncSQLClient.call(
              "INSERT INTO session VALUES ('id')",
              t -> {
                System.out.println(t);
                sink.success(null);
              });
        });
  }

  @Override
  public Mono<PostgresSession> findById(String s) {
    return null;
  }

  @Override
  public Mono<Void> deleteById(String s) {
    return null;
  }

  final class PostgresSession implements Session {

    @Override
    public String getId() {
      return null;
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
