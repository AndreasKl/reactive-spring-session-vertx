package net.andreaskluth.reactivesession

import io.reactiverse.pgclient.PgPoolOptions
import net.andreaskluth.session.postgres.ReactivePostgresSessionConfiguration
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

@SpringBootApplication
@Import(ReactivePostgresSessionConfiguration::class)
class PostgresSessionDemoApplication

@Configuration
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


@Controller
class HelloController {

    @GetMapping("/")
    fun hello(): ResponseEntity<Mono<String>> = ResponseEntity.ok(Mono.just("hallo"))
}

fun main(args: Array<String>) {
    runApplication<PostgresSessionDemoApplication>(*args)
}
