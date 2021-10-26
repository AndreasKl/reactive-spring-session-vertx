package net.andreaskluth.session.mysql.testsupport;

import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;

public class TestMySQLOptions {

  private TestMySQLOptions() {
    // Construction not permitted.
  }

  private static MySQLConnectOptions mySQLConnectOptions(int port) {
    return new MySQLConnectOptions()
        .setPort(port)
        .setHost("localhost")
        .setDatabase("session")
        .setUser("mysql")
        .setPassword("mysql");
  }

  private static PoolOptions poolOptions() {
    return new PoolOptions().setMaxSize(5).setMaxWaitQueueSize(100);
  }

  public static MySQLPool pool(int port) {
    return MySQLPool.pool(mySQLConnectOptions(port), poolOptions());
  }
}
