package net.andreaskluth.session.core.support;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.vertx.sqlclient.Pool;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.session.core.ReactiveSessionException;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ReactiveSessionSchemaPopulator {

  private ReactiveSessionSchemaPopulator() {
    // Construction is not permitted.
  }

  public static void applyDefaultSchema(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String sqlStatement : parseStatementsFromSchema()) {
        statement.execute(sqlStatement);
      }
    }
  }

  public static Mono<Void> applyDefaultSchema(Pool pool) {
    return applySchema(pool, parseStatementsFromSchema());
  }

  public static Mono<Void> applySchema(Pool pool, String[] statements) {
    Mono<Void> result = Mono.empty();
    for (String statement : statements) {
      result = result.then(Mono.create(sink -> applyStatement(pool, statement, sink)));
    }
    return result;
  }

  private static void applyStatement(Pool pool, String statement, MonoSink<Void> sink) {
    pool.query(statement)
        .execute(
            event -> {
              if (event.succeeded()) {
                sink.success();
                return;
              }
              sink.error(event.cause());
            });
  }

  public static String[] parseStatementsFromSchema() {
    try (InputStream schemaStream =
        ReactiveSessionSchemaPopulator.class.getClassLoader().getResourceAsStream("schema.sql")) {
      return StringUtils.split(StreamUtils.copyToString(schemaStream, UTF_8), ";");
    } catch (IOException e) {
      throw new ReactiveSessionException("Failed to read schema.sql.", e);
    }
  }
}
