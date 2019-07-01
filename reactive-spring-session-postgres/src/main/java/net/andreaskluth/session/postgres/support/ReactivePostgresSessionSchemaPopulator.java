package net.andreaskluth.session.postgres.support;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.session.postgres.ReactivePostgresSessionException;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

public class ReactivePostgresSessionSchemaPopulator {

  private ReactivePostgresSessionSchemaPopulator() {
    // Construction is not permitted.
  }

  public static void applySchema(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String sqlStatement : parseStatementsFromSchema()) {
        statement.execute(sqlStatement);
      }
    }
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
