package pessimistic

import org.junit.AfterClass
import org.junit.jupiter.api.Test
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.DriverManager
import kotlin.concurrent.thread

@Testcontainers
class PessimisticLocksTest {


    @Test
    fun testExclusiveLock() {
        val transaction1 = buildTransactionExecutor("FIRST", "red")
        val transaction2 = buildTransactionExecutor("SECOND", "green")

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
        val transaction1 = buildTransactionExecutor("FIRST", "red")
        val transaction2 = buildTransactionExecutor("SECOND", "green")

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

    @Test
    fun testDeadlock() {
        val transaction1 = buildTransactionExecutor("FIRST", "red")
        val transaction2 = buildTransactionExecutor("SECOND", "green")

        val first = thread {
            val connection = connection()
            val openConnection = transaction1.executeDeadLockExample(1, 5_000, connection = connection)
            transaction1.executeDeadLockExample(2, shouldCommit = true, connection = openConnection)
        }

        Thread.sleep(1_000)

        val second = thread {
            val connection = connection()
            val openConnection = transaction2.executeDeadLockExample(2, connection = connection)
            transaction2.executeDeadLockExample(1, shouldCommit = true, connection = openConnection)
        }

        first.join()
        second.join()
    }

    private fun connection(): Connection? {
        return DriverManager.getConnection(
            postgresContainer.jdbcUrl,
            postgresContainer.username,
            postgresContainer.password
        )
    }

    private fun buildTransactionExecutor(transactionName: String, color: String) = PessimisticLocks(
        postgresContainer.jdbcUrl,
        postgresContainer.username,
        postgresContainer.password,
        transactionName,
        color
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
        fun destroy() {
            postgresContainer.close()
        }
    }

}
