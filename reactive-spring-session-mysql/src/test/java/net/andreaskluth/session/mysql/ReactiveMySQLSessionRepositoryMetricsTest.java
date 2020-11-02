package net.andreaskluth.session.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.sqlclient.Pool;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Set;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository.ReactiveSession;
import net.andreaskluth.session.core.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.mysql.testsupport.MySQLDbExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ReactiveMySQLSessionRepositoryMetricsTest {

  private Pool pool = null;

  @RegisterExtension static final MySQLDbExtension embeddedMySQL = new MySQLDbExtension();

  @BeforeEach
  void before() {
    Set<MeterRegistry> registries = Metrics.globalRegistry.getRegistries();
    registries.forEach(Metrics.globalRegistry::remove);

    pool = pool();
  }

  @AfterEach
  void after() {
    pool.close();

    Metrics.globalRegistry.close();
  }

  @Test
  void saveAndLoad() {
    SimpleMeterRegistry simple = new SimpleMeterRegistry(SimpleConfig.DEFAULT, new MockClock());
    Metrics.addRegistry(simple);

    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();
    repo.findById(session.getId()).block();
    repo.cleanupExpiredSessions().block();
    repo.deleteById(session.getId()).block();

    assertThatCallWasMetered("createSession", "completed");
    assertThatCallWasMetered("save", "completedEmpty");
    assertThatCallWasMetered("findById", "completed");
    assertThatCallWasMetered("cleanupExpiredSessions", "completed");
    assertThatCallWasMetered("deleteById", "completedEmpty");
  }

  private void assertThatCallWasMetered(String method, String status) {
    assertThat(
            Metrics.globalRegistry
                .get("ReactiveMySQLSessionRepository.flow.duration")
                .tag("status", status)
                .tag("method", method)
                .timer()
                .count())
        .isEqualTo(1L);
  }

  private ReactiveVertxSessionRepository sessionRepository() {
    ReactiveVertxSessionRepository sessionRepository =
        new ReactiveVertxSessionRepository(
            pool,
            new ReactiveMySQLSessionRepositoryQueries(),
            new JdkSerializationStrategy(),
            Clock.system(ZoneId.systemDefault()));
    sessionRepository.withMetrics(true);
    sessionRepository.setMetricSequenceName("ReactiveMySQLSessionRepository");
    return sessionRepository;
  }

  private Pool pool() {
    return TestMySQLOptions.pool(embeddedMySQL.getPort());
  }
}
