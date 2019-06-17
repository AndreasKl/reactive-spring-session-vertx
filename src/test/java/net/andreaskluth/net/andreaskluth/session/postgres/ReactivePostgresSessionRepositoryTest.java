package net.andreaskluth.net.andreaskluth.session.postgres;

import static io.vertx.ext.asyncsql.PostgreSQLClient.DEFAULT_DATABASE;
import static io.vertx.ext.asyncsql.PostgreSQLClient.DEFAULT_PORT;
import static io.vertx.ext.asyncsql.PostgreSQLClient.DEFAULT_USER;
import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import java.nio.charset.Charset;
import java.sql.Statement;
import java.util.Map;
import net.andreaskluth.net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import org.junit.Rule;
import org.junit.Test;

public class ReactivePostgresSessionRepositoryTest {

  @Rule
  public PreparedDbRule pg =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (var con = ds.getConnection();
                Statement stmt = con.createStatement(); ) {
              stmt.execute("CREATE DATABASE session;");
              stmt.execute("CREATE TABLE session (id text);");
            }
          });

  @Test
  public void saveAndLoadWorks() {
    AsyncSQLClient asyncSQLClient = postgresClient();
    ReactivePostgresSessionRepository repo = new ReactivePostgresSessionRepository(asyncSQLClient);

    PostgresSession session = repo.createSession().block();
    session.setAttribute("key", "value");

    String sessionId = session.getId();

    repo.save(session).block();

    PostgresSession loadedSession = repo.findById(sessionId).block();

    assertThat(loadedSession.getId()).isEqualTo(sessionId);
    assertThat(loadedSession.<String>getAttribute("key")).isEqualTo("value");
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

    Vertx vertx = Vertx.vertx();
    Map<String, Object> config =
        Map.of(
            "database",
            "session",
            "username",
            "postgres",
            "port",
            pg.getConnectionInfo().getPort());
    JsonObject jsonConfig = new JsonObject(config);

    return PostgreSQLClient.createNonShared(vertx, jsonConfig);
  }
}
