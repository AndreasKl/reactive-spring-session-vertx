package net.andreaskluth.net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import reactor.core.publisher.Mono;

public class ReactivePostgresSessionRepositoryTest {

  private AsyncSQLClient asyncSQLClient = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection();
                Statement statement = connection.createStatement(); ) {
              statement.execute(
                  "CREATE TABLE session"
                      + " ( "
                      + "   id text PRIMARY KEY,"
                      + "   session_id text NOT NULL,"
                      + "   session_data text,"
                      + "   creation_time INT8 NOT NULL,"
                      + "   last_accessed_time INT8  NOT NULL,"
                      + "   expiry_time INT8 NOT NULL,"
                      + "   max_inactive_interval INT4 NOT NULL"
                      + " );");
              statement.execute("CREATE INDEX session_session_id_idx ON session (session_id);");
            }
          });

  @Before
  public void before() {
    asyncSQLClient = postgresClient();
  }

  @After
  public void after() {
    asyncSQLClient.close();
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
    // FIXME: This test should have failed, both save calls try to insert a new record and the last one fails with a duplicate
    // key issue. However due to whatever reasons (no auto commit?) no commit is executed.

    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    Mono<Void> saveA = repo.save(session);

    session.setAttribute("keyB", "value B");
    Mono<Void> saveB = repo.save(session);

    Mono.zip(saveA, saveB).block();

    var reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  private ReactivePostgresSessionRepository sessionRepository() {
    return new ReactivePostgresSessionRepository(
        asyncSQLClient,
        new SerializationStrategy(),
        new DeserializationStrategy(),
        Clock.systemDefaultZone());
  }

  private AsyncSQLClient postgresClient() {

    //    String host = config.getString("host", defaultHost);
    //    int port = config.getInteger("port", defaultPort);
    //    String username = config.getString("username", defaultUser);
    //    String password = config.getString("password", defaultPassword);
    //    String database = config.getString("database", defaultDatabase);
    //    Charset charset = Charset.forName(config.getString("charset", defaultCharset));
    //    long connectTimeout = config.getLong("connectTimeout", defaultConnectTimeout);
    //    long testTimeout = config.getLong("testTimeout", defaultTestTimeout);
    //    Long queryTimeout = config.getLong("queryTimeout");

    var vertx = Vertx.vertx();
    var postgresConfig = new JsonObject(postgresConfig());
    return PostgreSQLClient.createNonShared(vertx, postgresConfig);
  }

  private Map<String, Object> postgresConfig() {
    return Map.of(
        "database",
        "template1",
        "username",
        "postgres",
        "password",
        "postgres",
        "port",
        embeddedPostgres.getConnectionInfo().getPort());
  }
}
