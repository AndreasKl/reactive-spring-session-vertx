package net.andreaskluth.net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactivePostgresSessionRepositoryTest {

  private PgPool pgPool = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection();
                Statement statement = connection.createStatement(); ) {
              statement.execute(
                  "CREATE TABLE session"
                      + " ( "
                      + "   id UUID PRIMARY KEY,"
                      + "   session_id text NOT NULL,"
                      + "   session_data bytea,"
                      + "   creation_time INT8 NOT NULL,"
                      + "   last_accessed_time INT8  NOT NULL,"
                      + "   expiry_time INT8 NOT NULL,"
                      + "   max_inactive_interval INT4 NOT NULL"
                      + " );");
              statement.execute("CREATE INDEX session_session_id_idx ON session (session_id);");
              statement.execute(
                  "CREATE INDEX session_expiry_time_idx ON session (expiry_time) WHERE expiry_time >= 0;");
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
}
