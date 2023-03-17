package pessimistic

import org.junit.AfterClass
import org.junit.jupiter.api.Test
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.concurrent.thread

@Testcontainers
class PessimisticLocksTest {


    @Test
    fun testExclusiveLock() {
        val transaction1 = buildExecutor("FIRST")
        val transaction2 = buildExecutor("SECOND")

        val first = thread {
            transaction1.executeExclusiveLockExample(5_000)
        }

        Thread.sleep(1_000)

        val second = thread {
            transaction2.executeExclusiveLockExample()
        }

        first.join()
        second.join()
    }

    @Test
    fun testSharedLock() {
        val transaction1 = buildExecutor("FIRST")
        val transaction2 = buildExecutor("SECOND")

        val first = thread {
            transaction1.executeSharedLockExample(5_000)
        }

        Thread.sleep(1_000)

        val second = thread {
            transaction2.executeSharedLockExample()
        }

        first.join()
        second.join()
    }

    private fun buildExecutor(transactionName: String) = PessimisticLocks(
        postgresContainer.jdbcUrl,
        postgresContainer.username,
        postgresContainer.password,
        transactionName
    )

    companion object {
        @Container
        var postgresContainer: JdbcDatabaseContainer<*> = PostgreSQLContainer("postgres:13.4")
            .withDatabaseName("locks")
            .withUsername("shardik")
            .withPassword("shardik")
            .withInitScript("init.sql")

        @JvmStatic
        @AfterClass
        fun destroy(): Unit {
            postgresContainer.close()
        }
    }

}
