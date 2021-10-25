[![Build Status](https://app.travis-ci.com/AndreasKl/reactive-spring-session-vertx.svg?branch=master)](https://app.travis-ci.com/AndreasKl/reactive-spring-session-vertx)
[![Maven Central](https://img.shields.io/maven-central/v/net.andreaskluth/reactive-spring-session-postgres.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22net.andreaskluth%22%20AND%20a:%22reactive-spring-session-postgres%22)
# reactive-spring-session-vertx

Due to the lack of a distributed reactive Spring Session implementations for relational databases,
this projects aims to fill the gap with a fully reactive **PostgreSQL** any **MySQL** Spring Session store.

The implementation is based on **[vertx-sql-client](https://github.com/eclipse-vertx/vertx-sql-client)**.

Supports JDK 8 and up. Does not depend on **Spring Boot** but can be easily used with **Spring Boot**.

### A few words on MySQL
The MySQL flavour is compatible with MySQL 5.7 and up. It is fully working but not battle tested on production like the PostgreSQL version.

## Setup instructions
A demo project is located under `/webflux-demo-application` written in Kotlin.
`ReactivePostgresSessionConfiguration` registers a `ReactivePostgresSessionRepository` 
and a scheduled task that removes expired sessions from the database. Expired sessions
are never returned to the user, however could remain in the database until the scheduler
does the cleanup.

```kotlin
@Configuration
@Import(ReactivePostgresSessionConfiguration::class)
class PostgresSessionConfiguration {

  @Bean
  fun pgConnectOptions(@Value("\${postgres.port}") postgresPort: Int): PgConnectOptions =
    PgConnectOptions()
      .setHost("localhost")
      .setPort(postgresPort)
      .setDatabase("session")
      .setUser("postgres")
      .setPassword("postgres")
      .setIdleTimeout(300)
      .setConnectTimeout(500)

  @Bean
  fun poolOptions(): PoolOptions =
    PoolOptions()
      .setMaxSize(5)
      .setMaxWaitQueueSize(10)

  @Bean
  fun clock(): Clock =
    Clock.systemUTC()

  @Bean(WebHttpHandlerBuilder.WEB_SESSION_MANAGER_BEAN_NAME)
  fun webSessionManager(repository: ReactiveSessionRepository<out Session>): WebSessionManager {
    val sessionStore = SpringSessionWebSessionStore(repository)

    val manager = DefaultWebSessionManager()
    manager.sessionStore = sessionStore
    return manager
  }

}
```

## Is this production ready?
**reactive-spring-session-vertx** is used in production **[@REWE digital](https://www.rewe-digital.com/)** for their Spring Gateway based reactive API gateway.

## Build

Just test and build, will start Postgres and MySQL as embedded databases:
```bash
mvn clean verify
```

Deploy the current SNAPSHOT to **oss.sonatype.org**:
```bash
mvn -P release -pl reactive-spring-session-postgres -am clean deploy
```

Deploy a RELEASE to **oss.sonatype.org**:
```bash
mvn release:prepare

mvn -P release clean deploy

mvn -P release nexus-staging:rc-list

mvn -P release -DstagingRepositoryId=netandreaskluth-<some-id>  nexus-staging:release
```

## Contributing
Source code formatting is checked with [fmt-maven-plugin](https://github.com/coveooss/fmt-maven-plugin). Configuration files for the formatter of your preferred IDE can be found [here](https://github.com/google/styleguide) a plugin for IntelliJ IDEA can be found [here](https://plugins.jetbrains.com/plugin/8527-google-java-format).

# License 
The MIT License (https://opensource.org/licenses/MIT)
