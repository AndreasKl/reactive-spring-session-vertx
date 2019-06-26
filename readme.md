[![Build Status](https://travis-ci.org/AndreasKl/tomatenmark.svg?branch=master)](https://travis-ci.org/AndreasKl/tomatenmark) 
[![codecov](https://codecov.io/gh/AndreasKl/tomatenmark/branch/master/graph/badge.svg)](https://codecov.io/gh/AndreasKl/tomatenmark)

# tomatenmark / working title
Due to the lack of a distributed reactive Spring Session implementations for relational databases,
this projects aims to fill the gap with a fully reactive **postgres** Spring Session store.

Instead of faking reactive behaviour by wrapping JDBC into a thread pool,
the implementation is based on **[reactive-pg-client](https://www.julienviet.com/reactive-pg-client/)**
and will switch to **[vertx-sql-client](https://github.com/eclipse-vertx/vertx-sql-client)** once it is GA to support MySQL.

Runs on JDK 11 and JDK 12, if there is a need a JDK 8 version would be possible.

## How to use
A demo project is located under `/demo-application` written in Kotlin.
`ReactivePostgresSessionConfiguration` registers a `ReactivePostgresSessionRepository` 
and a scheduled task that removes expired sessions from the database. Expired sessions
are never returned to the user, however could remain in the database until the scheduler
does the cleanup.

```kotlin
@Configuration
@Import(ReactivePostgresSessionConfiguration::class)
class PostgresSessionConfiguration {

    @Bean
    fun pgPoolOptions(): PgPoolOptions =
            PgPoolOptions()
                    .setHost("localhost")
                    .setPort(5432)
                    .setDatabase("session")
                    .setUser("postgres")
                    .setPassword("postgres")

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


## Development State
tomatenmark is currently under development and probably not production ready.

## Build

Just test and build:
```bash
mvn clean verify
```

Deploy the current SNAPSHOT to **oss.sonatype.org**:
```bash
mvn -P release -pl reactive-spring-session-postgres clean deploy
```


# Licence 
Apache Licence 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
