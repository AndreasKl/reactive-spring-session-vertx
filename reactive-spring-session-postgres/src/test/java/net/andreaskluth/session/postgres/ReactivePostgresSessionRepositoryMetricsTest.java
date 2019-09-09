package net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.sqlclient.Pool;
import java.sql.Connection;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Set;
import net.andreaskluth.session.postgres.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.postgres.support.ReactivePostgresSessionSchemaPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ReactivePostgresSessionRepositoryMetricsTest {

  private Pool pgPool = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection()) {
              ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(connection);
            }
          });

  @Before
  public void before() {
    Set<MeterRegistry> registries = Metrics.globalRegistry.getRegistries();
    registries.forEach(Metrics.globalRegistry::remove);

    pgPool = pool();
  }

  @After
  public void after() {
    pgPool.close();

    Metrics.globalRegistry.close();
  }

  @Test
  public void saveAndLoad() {
    SimpleMeterRegistry simple = new SimpleMeterRegistry(SimpleConfig.DEFAULT, new MockClock());
    Metrics.addRegistry(simple);

    var repo = sessionRepository();
    var session = repo.createSession().block();
    repo.save(session).block();
    repo.findById(session.getId()).block();
    repo.cleanupExpiredSessions().block();
    repo.deleteById(session.getId()).block();

    assertThatCallWasMetered("createSession");
    assertThatCallWasMetered("save");
    assertThatCallWasMetered("findById");
    assertThatCallWasMetered("cleanupExpiredSessions");
    assertThatCallWasMetered("deleteById");
  }

  private void assertThatCallWasMetered(String save) {
    assertThat(
            Metrics.globalRegistry
                .get("reactor.flow.duration")
                .tag("status", "completed")
                .tag("flow", "ReactivePostgresSessionRepository")
                .tag("method", save)
                .timer()
                .count())
        .isEqualTo(1L);
  }

  private ReactivePostgresSessionRepository sessionRepository() {
    var sessionRepository =
        new ReactivePostgresSessionRepository(
            pgPool, new JdkSerializationStrategy(), Clock.system(ZoneId.systemDefault()));
    sessionRepository.withMetrics(true);
    return sessionRepository;
  }

  private Pool pool() {
    return TestPostgresOptions.pool(embeddedPostgres.getConnectionInfo().getPort());
  }
}
