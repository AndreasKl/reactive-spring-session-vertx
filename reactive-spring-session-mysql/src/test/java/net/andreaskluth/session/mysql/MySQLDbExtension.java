package net.andreaskluth.session.mysql;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.SchemaConfig;
import net.andreaskluth.session.core.support.ReactiveSessionSchemaPopulator;
import org.junit.jupiter.api.extension.Extension;

public class MySQLDbExtension implements Extension {

  private static final EmbeddedMysql mySQLd =
      anEmbeddedMysql(config())
          .addSchema(
              SchemaConfig.aSchemaConfig("session")
                  .withCommands(ReactiveSessionSchemaPopulator.parseStatementsFromSchema())
                  .build())
          .start();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(mySQLd::stop));
  }

  private static MysqldConfig config() {
    return aMysqldConfig(v5_7_latest).withUser("mysql", "mysql").build();
  }

  public int getPort() {
    return mySQLd.getConfig().getPort();
  }
}
