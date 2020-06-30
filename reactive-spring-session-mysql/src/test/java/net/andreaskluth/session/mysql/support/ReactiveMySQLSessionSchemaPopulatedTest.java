package net.andreaskluth.session.mysql.support;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.vertx.mysqlclient.MySQLException;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import net.andreaskluth.session.core.MonoToVertxHandlerAdapter;
import net.andreaskluth.session.core.support.ReactiveSessionSchemaPopulator;
import net.andreaskluth.session.mysql.TestMySQLOptions;
import net.andreaskluth.session.mysql.testsupport.MySQLDbExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;

class ReactiveMySQLSessionSchemaPopulatedTest {

  @RegisterExtension static final MySQLDbExtension embeddedMySQL = new MySQLDbExtension();

  private static final String[] DEFECTIVE_SCHEMA = {
    "CREATE TABLE demo (id text);", "CREATE TABLE demo (id text);"
  };

  @Test
  void schemaIsCreated() {
    Mono.<RowSet<Row>>create(
            sink -> {
              MonoToVertxHandlerAdapter<RowSet<Row>> adapter =
                  new MonoToVertxHandlerAdapter<>(sink);
              pool().query("DROP TABLE IF EXISTS session").execute(adapter);
            })
        .block();

    ReactiveSessionSchemaPopulator.applyDefaultSchema(pool()).block();

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
            () -> ReactiveSessionSchemaPopulator.applySchema(pool(), DEFECTIVE_SCHEMA).block())
        .isInstanceOf(MySQLException.class);
  }

  private MySQLPool pool() {
    return TestMySQLOptions.pool(embeddedMySQL.getPort());
  }
}
