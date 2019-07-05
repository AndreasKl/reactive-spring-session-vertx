package net.andreaskluth.reactivesession

import com.opentable.db.postgres.embedded.ConnectionInfo
import com.opentable.db.postgres.embedded.PreparedDbProvider
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgPoolOptions
import net.andreaskluth.session.postgres.ReactivePostgresSessionConfiguration
import net.andreaskluth.session.postgres.support.ReactivePostgresSessionSchemaPopulator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
import org.springframework.session.ReactiveSessionRepository
import org.springframework.session.Session
import org.springframework.session.web.server.session.SpringSessionWebSessionStore
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.server.adapter.WebHttpHandlerBuilder
import org.springframework.web.server.session.DefaultWebSessionManager
import org.springframework.web.server.session.WebSessionManager
import reactor.core.publisher.Mono
import java.time.Clock
import javax.annotation.PostConstruct

@SpringBootApplication
@Import(ReactivePostgresSessionConfiguration::class)
class PostgresSessionDemoApplication

@Configuration
class PostgresSessionConfiguration {

    @Bean
    fun pgPoolOptions(@Value("\${postgres.port}") postgresPort: Int): PgPoolOptions =
            PgPoolOptions()
                    .setHost("localhost")
                    .setPort(postgresPort)
                    .setDatabase("session")
                    .setUser("postgres")
                    .setPassword("postgres")
                    .setMaxSize(5)
                    .setMaxWaitQueueSize(10)
                    .setIdleTimeout(300)
                    .setConnectTimeout(500)

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

@Configuration
class PrepareSchemaConfiguration(val pgPool: PgPool) {

    @PostConstruct
    fun prepareSchema() {
        ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(pgPool).block()
    }
}

@Controller
class HelloController {

    @GetMapping("/")
    fun hello(): ResponseEntity<Mono<String>> = ResponseEntity.ok(Mono.just("hallo"))
}

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger(PostgresSessionDemoApplication::class.java)

    val provider = PreparedDbProvider.forPreparer({ ds ->
        ds.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE DATABASE session")
            }
        }
    }, listOf())
    val connInfo = provider.createNewDatabase()
    exportPort(log, connInfo)
    runApplication<PostgresSessionDemoApplication>(*args)
}

private fun exportPort(log: Logger, connInfo: ConnectionInfo) {
    System.setProperty("postgres.port", connInfo.port.toString())
    log.info("Running with embedded postgres on port: {}", connInfo.port)
}
