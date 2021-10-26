package net.andreaskluth.session.postgres.support;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import io.vertx.pgclient.PgException;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import net.andreaskluth.session.core.MonoToVertxHandlerAdapter;
import net.andreaskluth.session.core.support.ReactiveSessionSchemaInitializer;
import net.andreaskluth.session.postgres.testsupport.TestPostgresOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;

class ReactivePostgresSessionSchemaInitializerTest {

  @RegisterExtension
  static final PreparedDbExtension embeddedPostgres =
      EmbeddedPostgresExtension.preparedDatabase(ds -> {});

  private static final String[] DEFECTIVE_SCHEMA = {
    "CREATE TABLE demo (id text);", "CREATE TABLE demo (id text);"
  };

  @Test
  void schemaIsCreated() {
    ReactiveSessionSchemaInitializer.applyDefaultSchema(pool()).block();

    Mono.<RowSet<Row>>create(
            sink -> {
              MonoToVertxHandlerAdapter<RowSet<Row>> adapter =
                  new MonoToVertxHandlerAdapter<>(sink);
              pool().query("SELECT * FROM session").execute(adapter);
            })
        .block();
  }

  @Test
  void schemaCanBeAppliedMultipleTimes() {
    ReactiveSessionSchemaInitializer.applyDefaultSchema(pool()).block();
    ReactiveSessionSchemaInitializer.applyDefaultSchema(pool()).block();
    ReactiveSessionSchemaInitializer.applyDefaultSchema(pool()).block();

    Mono.<RowSet<Row>>create(
            sink -> {
              MonoToVertxHandlerAdapter<RowSet<Row>> adapter =
                  new MonoToVertxHandlerAdapter<>(sink);
              pool().query("SELECT * FROM session").execute(adapter);
            })
        .block();
  }

  @Test
  void failsIfStatementsCanNotBeExecuted() {
    assertThatThrownBy(
            () -> ReactiveSessionSchemaInitializer.applySchema(pool(), DEFECTIVE_SCHEMA).block())
        .isInstanceOf(PgException.class);
  }

  private PgPool pool() {
    return TestPostgresOptions.pool(embeddedPostgres.getConnectionInfo().getPort());
  }
}
