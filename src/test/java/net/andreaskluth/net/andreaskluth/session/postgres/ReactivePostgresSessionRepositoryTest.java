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
import java.util.Map;
import net.andreaskluth.net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ReactivePostgresSessionRepositoryTest {

  private AsyncSQLClient asyncSQLClient = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection();
                Statement statement = connection.createStatement(); ) {
              statement.execute("CREATE TABLE session (id text PRIMARY KEY, session_data text);");
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
  public void saveAndLoadWorks() {
    ReactivePostgresSessionRepository repo = sessionRepository();

    PostgresSession session = repo.createSession().block();
    session.setAttribute("key", "value");

    String sessionId = session.getId();

    repo.save(session).block();

    PostgresSession loadedSession = repo.findById(sessionId).block();

    assertThat(loadedSession.getId()).isEqualTo(sessionId);
    assertThat(loadedSession.<String>getAttribute("key")).isEqualTo("value");
  }

  private ReactivePostgresSessionRepository sessionRepository() {
    return new ReactivePostgresSessionRepository(
        asyncSQLClient, new SerializationStrategy(), new DeserializationStrategy());
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
