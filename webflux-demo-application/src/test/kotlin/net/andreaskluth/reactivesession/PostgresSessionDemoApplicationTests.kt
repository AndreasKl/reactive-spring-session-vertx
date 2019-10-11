package net.andreaskluth.reactivesession

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.PreparedDbRule
import org.assertj.core.api.Assertions.assertThat
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.reactive.function.BodyInserters

@RunWith(SpringRunner::class)
@SpringBootTest
@AutoConfigureWebTestClient
class PostgresSessionDemoApplicationTests {

    @Autowired
    lateinit var webTestClient: WebTestClient
    @Autowired
    lateinit var userDetailsService: MapReactiveUserDetailsService

    companion object {

        @ClassRule
        @JvmField
        val embeddedPostgres: PreparedDbRule = EmbeddedPostgresRules.preparedDatabase { ds ->
            ds.connection.use { con ->
                con.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE session;")
                }
            }
        }

        @BeforeClass
        @JvmStatic
        fun setup() {
            System.setProperty("postgres.port", embeddedPostgres.connectionInfo.port.toString())
        }
    }

    @Test
    fun appIsSecured() {
        webTestClient.get().uri("/").exchange().expectStatus().isUnauthorized
    }

    @Test
    fun loginWorks() {
        val user = userDetailsService.findByUsername("user").block()

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

    private fun loginFormBody(user: UserDetails?) =
            BodyInserters.fromFormData("username", user!!.username).with("password", sanitizePassword(user))

    private fun sanitizePassword(user: UserDetails) = user.password.removePrefix("{noop}")

}
