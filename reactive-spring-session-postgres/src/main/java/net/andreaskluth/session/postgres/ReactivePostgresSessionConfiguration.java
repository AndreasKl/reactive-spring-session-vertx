package net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.time.Clock;
import java.util.Optional;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository;
import net.andreaskluth.session.core.ReactiveVertxSessionRepositoryQueries;
import net.andreaskluth.session.core.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.core.serializer.SerializationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import reactor.core.scheduler.Schedulers;

@Configuration
@EnableScheduling
public class ReactivePostgresSessionConfiguration implements SchedulingConfigurer {

  public static final String DEFAULT_EVERY_MINUTE_CLEANUP_CRON = "0 * * * * *";

  private final Clock clock;
  private final PoolOptions poolOptions;
  private final PgConnectOptions pgConnectOptions;

  public ReactivePostgresSessionConfiguration(
      PgConnectOptions pgConnectOptions, PoolOptions poolOptions, Optional<Clock> clock) {
    this.pgConnectOptions = requireNonNull(pgConnectOptions, "pgConnectOptions must not be null");
    this.poolOptions = requireNonNull(poolOptions, "poolOptions must not be null");
    this.clock =
        requireNonNull(clock, "clock must not be null").orElseGet(Clock::systemDefaultZone);
  }

  @Bean
  public SerializationStrategy reactiveSerializationStrategy() {
    return new JdkSerializationStrategy();
  }

  @Bean
  public Pool pool() {
    return PgPool.pool(pgConnectOptions, poolOptions);
  }

  @Bean
  public ReactiveVertxSessionRepository reactivePostgresSessionRepository() {
    ReactiveVertxSessionRepository reactiveVertxSessionRepository =
        new ReactiveVertxSessionRepository(
            pool(), queries(), reactiveSerializationStrategy(), clock);
    reactiveVertxSessionRepository.setMetricSequenceName("ReactivePostgresSessionRepository");
    return reactiveVertxSessionRepository;
  }

  private ReactiveVertxSessionRepositoryQueries queries() {
    return new ReactivePostgresSessionRepositoryQueries();
  }

  @Override
  public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
    scheduledTaskRegistrar.addCronTask(
        () ->
            reactivePostgresSessionRepository()
                .cleanupExpiredSessions()
                .subscribeOn(Schedulers.immediate())
                .subscribe(),
        DEFAULT_EVERY_MINUTE_CLEANUP_CRON);
  }
}
