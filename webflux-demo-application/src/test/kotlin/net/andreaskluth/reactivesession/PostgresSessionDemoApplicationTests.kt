package net.andreaskluth.reactivesession

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest
class PostgresSessionDemoApplicationTests {

    companion object {

        @ClassRule
        @JvmField
        val embeddedPostgres = EmbeddedPostgresRules.preparedDatabase { ds ->
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
    fun contextLoads() {
    }

}
