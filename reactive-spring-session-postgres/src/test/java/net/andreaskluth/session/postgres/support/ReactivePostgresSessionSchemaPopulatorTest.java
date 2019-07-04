package net.andreaskluth.session.postgres.support;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgException;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import org.junit.ClassRule;
import org.junit.Test;
import reactor.core.publisher.Mono;

public class ReactivePostgresSessionSchemaPopulatorTest {

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(ds -> {});

  private static final String[] DEFECTIVE_SCHEMA = {
    "CREATE TABLE demo (id text);", "CREATE TABLE demo (id text);"
  };

  @Test
  public void schemaIsCreated() {
    ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(pool()).block();

    Mono.create(
            sink ->
                pool()
                    .query(
                        "SELECT * FROM session",
                        event -> {
                          if (event.succeeded()) {
                            sink.success();
                            return;
                          }
                          sink.error(event.cause());
                        }))
        .block();
  }

  @Test
  public void schemaCanBeAppliedMultipleTimes() {
    ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(pool()).block();
    ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(pool()).block();
    ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(pool()).block();

    Mono.create(
            sink ->
                pool()
                    .query(
                        "SELECT * FROM session",
                        event -> {
                          if (event.succeeded()) {
                            sink.success();
                            return;
                          }
                          sink.error(event.cause());
                        }))
        .block();
  }

  @Test
  public void failsIfStatementsCanNotBeExecuted() {
    assertThatThrownBy(
            () ->
                ReactivePostgresSessionSchemaPopulator.applySchema(pool(), DEFECTIVE_SCHEMA)
                    .block())
        .isInstanceOf(PgException.class);
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
