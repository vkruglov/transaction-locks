package pessimistic

import java.sql.DriverManager
import java.sql.ResultSet


class PessimisticLocks(
    private val jdbcUrl: String,
    private val username: String,
    private val password: String,
    private val transactionName: String
) {

    fun executeExclusiveLockExample(waitTime: Int? = null) {
        execute("select * from person where id = 1 for update ", waitTime)
    }

    fun executeSharedLockExample(waitTime: Int? = null) {
        execute("select * from person where id = 1 for share ", waitTime)
    }

    private fun execute(selectQuery: String, waitTime: Int? = null) {
        DriverManager.getConnection(jdbcUrl, username, password).use { conn ->
            log("Connected to DB", transactionName)

            conn.autoCommit = false

            conn.prepareStatement(selectQuery).use { stmt ->
                log("Acquiring lock", transactionName)
                stmt.executeQuery().use { rs ->
                    log("Acquired lock", transactionName)
                    sleep(waitTime)
                    logResults(rs)
                }
            }

            conn.commit()
        }
    }

    private fun sleep(waitTime: Int?) {
        waitTime?.let {
            log("Will sleep for $waitTime", transactionName)
            Thread.sleep(it.toLong())
            log("Done sleeping", transactionName)
        }
    }

    private fun logResults(rs: ResultSet) {
        while (rs.next()) {
            val id = rs.getInt("id")
            val firstName = rs.getString("first_name")
            val lastName = rs.getString("last_name")
            log("ID: $id first name: $firstName last name: $lastName", transactionName)
        }
    }

    private fun log(message: String, transactionName: String) {
        println("$transactionName :: $message")
    }
}
