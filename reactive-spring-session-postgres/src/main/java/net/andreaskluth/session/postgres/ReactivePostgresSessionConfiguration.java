package net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import java.time.Clock;
import java.util.Optional;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import reactor.core.scheduler.Schedulers;

@Configuration
@EnableScheduling
public class ReactivePostgresSessionConfiguration implements SchedulingConfigurer {

  public static final String DEFAULT_CLEANUP_CRON = "0 * * * * *";

  private final Clock clock;
  private final PgPoolOptions pgPoolOptions;

  private String cleanupCron = DEFAULT_CLEANUP_CRON;

  public ReactivePostgresSessionConfiguration(PgPoolOptions pgPoolOptions, Optional<Clock> clock) {
    this.pgPoolOptions = requireNonNull(pgPoolOptions, "pgPoolOptions must not be null");
    this.clock =
        requireNonNull(clock, "clock must not be null").orElseGet(Clock::systemDefaultZone);
  }

  public void setCleanupCron(String cleanupCron) {
    this.cleanupCron = cleanupCron;
  }

  @Bean
  public DeserializationStrategy reactivePostgresSessionDeserializationStrategy() {
    return new DeserializationStrategy();
  }

  @Bean
  public SerializationStrategy reactivePostgresSessionSerializationStrategy() {
    return new SerializationStrategy();
  }

  @Bean
  public PgPool pgPool() {
    return PgClient.pool(pgPoolOptions);
  }

  @Bean
  public ReactivePostgresSessionRepository reactivePostgresSessionRepository() {
    return new ReactivePostgresSessionRepository(
        pgPool(),
        reactivePostgresSessionSerializationStrategy(),
        reactivePostgresSessionDeserializationStrategy(),
        clock);
  }

  @Override
  public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
    scheduledTaskRegistrar.addCronTask(
        () ->
            reactivePostgresSessionRepository()
                .cleanupExpiredSessions()
                .subscribeOn(Schedulers.immediate())
                .subscribe(),
        this.cleanupCron);
  }
}
