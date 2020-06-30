package net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
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
import net.andreaskluth.session.core.ReactiveVertxSessionRepository;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository.ReactiveSession;
import net.andreaskluth.session.core.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.core.support.ReactiveSessionSchemaPopulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ReactivePostgresSessionRepositoryMetricsTest {

  private Pool pool = null;

  @RegisterExtension
  static final PreparedDbExtension embeddedPostgres =
      EmbeddedPostgresExtension.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection()) {
              ReactiveSessionSchemaPopulator.applyDefaultSchema(connection);
            }
          });

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

  private ReactiveVertxSessionRepository sessionRepository() {
    ReactiveVertxSessionRepository sessionRepository =
        new ReactiveVertxSessionRepository(
            pool,
            new ReactivePostgresSessionRepositoryQueries(),
            new JdkSerializationStrategy(),
            Clock.system(ZoneId.systemDefault()));
    sessionRepository.withMetrics(true);
    sessionRepository.setMetricSequenceName("ReactivePostgresSessionRepository");
    return sessionRepository;
  }

  private Pool pool() {
    return TestPostgresOptions.pool(embeddedPostgres.getConnectionInfo().getPort());
  }
}
