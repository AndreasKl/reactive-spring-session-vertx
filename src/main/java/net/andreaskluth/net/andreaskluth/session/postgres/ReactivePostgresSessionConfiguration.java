package net.andreaskluth.net.andreaskluth.session.postgres;

import static java.util.Objects.requireNonNull;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
@EnableScheduling
public class ReactivePostgresSessionConfiguration implements SchedulingConfigurer {

  public static final String DEFAULT_CLEANUP_CRON = "0 * * * * *";
  private final Clock clock;

  private String cleanupCron = DEFAULT_CLEANUP_CRON;

  public ReactivePostgresSessionConfiguration(Clock clock) {
    // FIXME: Provide clock if none is configured as bean.
    this.clock = requireNonNull(clock, "clock must not be null");
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
    // FIXME: Provide config object.
    PgPoolOptions options =
        new PgPoolOptions()
            .setPort(5432)
            .setHost("localhost")
            .setDatabase("template1")
            .setUser("postgres")
            .setPassword("postgres")
            .setMaxSize(5);
    return PgClient.pool(options);
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
        () -> reactivePostgresSessionRepository().cleanupExpiredSessions().block(),
        this.cleanupCron);
  }
}
