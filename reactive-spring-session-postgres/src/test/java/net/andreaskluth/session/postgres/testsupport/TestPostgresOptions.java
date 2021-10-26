package net.andreaskluth.session.postgres.testsupport;

import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

public class TestPostgresOptions {

  private TestPostgresOptions() {
    // Construction not permitted.
  }

  private static PgConnectOptions pgConnectOptions(int port) {
    return new PgConnectOptions()
        .setPort(port)
        .setHost("localhost")
        .setDatabase("template1")
        .setUser("postgres")
        .setPassword("postgres");
  }

  private static PoolOptions poolOptions() {
    return new PoolOptions().setMaxSize(5).setMaxWaitQueueSize(100);
  }

  public static PgPool pool(int port) {
    return PgPool.pool(pgConnectOptions(port), poolOptions());
  }
}
