package net.andreaskluth.session.postgres;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactivePostgresSessionRepositoryTest {

  private PgPool pgPool = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection();
                Statement statement = connection.createStatement()) {
              String[] sqlStatements = parseStatementsFromSchema();
              for (String sqlStatement : sqlStatements) {
                statement.execute(sqlStatement);
              }
            }
          });

  @Before
  public void before() {
    pgPool = pool();
  }

  @After
  public void after() {
    pgPool.close();
  }

  @Test
  public void saveAndLoadWithAttributes() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("key", "value");

    var sessionId = session.getId();

    repo.save(session).block();

    var loadedSession = repo.findById(sessionId).block();

    assertThat(loadedSession.getId()).isEqualTo(sessionId);
    assertThat(loadedSession.<String>getAttribute("key")).isEqualTo("value");
  }

  @Test
  public void rotateSessionId() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    var sessionId = session.getId();
    String changedSessionId = session.changeSessionId();

    repo.save(session).block();

    var loadedSession = repo.findById(changedSessionId).block();

    assertThat(sessionId).isNotEqualTo(changedSessionId);
    assertThat(loadedSession.getId()).isEqualTo(changedSessionId);
  }

  @Test
  public void updatingValuesInASession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("key", "value");

    repo.save(session).block();

    var reloadedSession = repo.findById(session.getId()).block();

    reloadedSession.setAttribute("key", "another value");

    repo.save(reloadedSession).block();

    assertThat(reloadedSession.<String>getAttribute("key")).isEqualTo("another value");
  }

  @Test
  public void storeComplexObjectsInSession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("key", new Complex(Instant.MAX));

    repo.save(session).block();

    var reloadedSession = repo.findById(session.getId()).block();

    assertThat(reloadedSession.<Complex>getAttribute("key")).isEqualTo(new Complex(Instant.MAX));
  }

  @Test
  public void savingMultipleTimes() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    Mono<Void> saveA = repo.save(session);
    saveA.block();

    session.setAttribute("keyB", "value B");
    Mono<Void> saveB = repo.save(session);
    saveB.block();

    var reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  public void savingInParallel() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    var saveA = repo.save(session);

    session.setAttribute("keyB", "value B");
    var saveB = repo.save(session);

    Flux.concat(saveA, saveB).blockLast();

    var reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  public void expiredSessionsCanNotBeRetrieved() {
    var repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(0);

    var session = repo.createSession().block();
    repo.save(session).block();
    assertThat(session.isExpired()).isTrue();

    var loadedSession = repo.findById(session.getId()).block();
    assertThat(loadedSession).isNull();
  }

  @Test
  public void expiredSessionsArePurgedByCleanup() {
    var repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(0);

    var session = repo.createSession().block();
    repo.save(session).block();

    Integer count = repo.cleanupExpiredSessions().block();

    assertThat(count).isGreaterThan(0);
  }

  private ReactivePostgresSessionRepository sessionRepository() {
    return new ReactivePostgresSessionRepository(
        pgPool,
        new SerializationStrategy(),
        new DeserializationStrategy(),
        Clock.systemDefaultZone());
  }

  private PgPool pool() {
    PgPoolOptions options =
        new PgPoolOptions()
            .setPort(embeddedPostgres.getConnectionInfo().getPort())
            .setHost("localhost")
            .setDatabase("template1")
            .setUser("postgres")
            .setPassword("postgres")
            .setMaxSize(5);
    return PgClient.pool(options);
  }

  private static String[] parseStatementsFromSchema() {
    try (InputStream schemaStream =
        ReactivePostgresSessionRepositoryTest.class
            .getClassLoader()
            .getResourceAsStream("schema.sql")) {
      return StringUtils.split(StreamUtils.copyToString(schemaStream, UTF_8), ";");
    } catch (IOException e) {
      throw new RuntimeException("Failed to read schema.sql.", e);
    }
  }

  private static class Complex implements Serializable {

    private static final long serialVersionUID = 1;

    private final Instant instant;

    private Complex(Instant instant) {
      this.instant = instant;
    }

    public Instant getInstant() {
      return instant;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Complex complex = (Complex) o;
      return Objects.equals(instant, complex.instant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(instant);
    }
  }
}
