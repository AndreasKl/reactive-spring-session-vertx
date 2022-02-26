package net.andreaskluth.reactivesession

import io.zonky.test.db.postgres.junit5.EmbeddedPostgresExtension
import io.zonky.test.db.postgres.junit5.PreparedDbExtension
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.reactive.function.BodyInserters

@SpringBootTest
@AutoConfigureWebTestClient
class PostgresSessionDemoApplicationTest {

    companion object {

        @RegisterExtension
        @JvmField
        val embeddedPostgres: PreparedDbExtension =
            EmbeddedPostgresExtension.preparedDatabase { ds ->
                ds.connection.use { con ->
                    con.createStatement().use { statement ->
                        statement.execute("CREATE DATABASE session;")
                    }
                }
            }

        @BeforeAll
        @JvmStatic
        fun setup() {
            System.setProperty("postgres.port", embeddedPostgres.connectionInfo.port.toString())
        }
    }


    @Autowired
    lateinit var webTestClient: WebTestClient

    @Autowired
    lateinit var userDetailsService: MapReactiveUserDetailsService

    @Test
    fun appIsSecured() {
        webTestClient.get().uri("/").exchange().expectStatus().isUnauthorized
    }

    @Test
    fun loginWorks() {
        val user: UserDetails =
            userDetailsService.findByUsername("user").block() ?: fail("User not found")

        webTestClient
            .mutateWith(csrf())
            .post().uri("/login")
            .body(loginFormBody(user))
            .exchange()
            .expectStatus().isFound
            .expectHeader().value(HttpHeaders.SET_COOKIE, this::assertHasSetNewSessionCookie)
            .expectHeader().valueEquals(HttpHeaders.LOCATION, "/")
    }

    private fun assertHasSetNewSessionCookie(t: String?) {
        assertThat(t).contains("SESSION")
    }

    private fun loginFormBody(user: UserDetails) =
        BodyInserters.fromFormData("username", user.username)
            .with("password", sanitizePassword(user))

    private fun sanitizePassword(user: UserDetails) = user.password.removePrefix("{noop}")

}
