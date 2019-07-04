package net.andreaskluth.session.postgres.support;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.reactiverse.pgclient.PgPool;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.session.postgres.ReactivePostgresSessionException;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ReactivePostgresSessionSchemaPopulator {

  private ReactivePostgresSessionSchemaPopulator() {
    // Construction is not permitted.
  }

  public static void applyDefaultSchema(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String sqlStatement : parseStatementsFromSchema()) {
        statement.execute(sqlStatement);
      }
    }
  }

  public static Mono<Void> applyDefaultSchema(PgPool pgPool) {
    return applySchema(pgPool, parseStatementsFromSchema());
  }

  public static Mono<Void> applySchema(PgPool pgPool, String[] statements) {
    Mono<Void> result = Mono.empty();
    for (String statement : statements) {
      result = result.then(Mono.create(sink -> applyStatement(pgPool, statement, sink)));
    }
    return result;
  }

  private static void applyStatement(PgPool pgPool, String statement, MonoSink<Void> sink) {
    pgPool.query(
        statement,
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
        ReactivePostgresSessionSchemaPopulator.class
            .getClassLoader()
            .getResourceAsStream("schema.sql")) {
      return StringUtils.split(StreamUtils.copyToString(schemaStream, UTF_8), ";");
    } catch (IOException e) {
      throw new ReactivePostgresSessionException("Failed to read schema.sql.", e);
    }
  }
}
