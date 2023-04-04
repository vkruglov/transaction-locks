package pessimistic

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

private const val EXCLUSIVE_LOCK_QUERY = "select * from person where id = ? for update"
private const val SHARED_LOCK_QUERY = "select * from person where id = ? for share"

class PessimisticLocks(
    private val jdbcUrl: String,
    private val username: String,
    private val password: String,
    private val transactionName: String,
    private val color: String
) {

    fun executeDeadLockExample(
        rowId: Int,
        waitTime: Int? = null,
        shouldCommit: Boolean? = false,
        connection: Connection? = null
    ): Connection {
        return execute(EXCLUSIVE_LOCK_QUERY, rowId, waitTime, shouldCommit, connection)
    }

    fun executeExclusiveLockExample(waitTime: Int? = null) {
        execute(EXCLUSIVE_LOCK_QUERY, 1, waitTime)
    }

    fun executeSharedLockExample(waitTime: Int? = null) {
        execute(SHARED_LOCK_QUERY, 1, waitTime)
    }

    private fun execute(
        selectQuery: String,
        rowId: Int,
        waitTime: Int? = null,
        shouldCommit: Boolean? = true,
        existingConnection: Connection? = null
    ): Connection {
        return if (existingConnection != null) executeQuery(
            existingConnection,
            selectQuery,
            rowId,
            waitTime,
            shouldCommit
        )
        else DriverManager.getConnection(jdbcUrl, username, password).use { conn ->
            executeQuery(conn, selectQuery, rowId, waitTime, shouldCommit)
        }
    }

    private fun executeQuery(
        conn: Connection,
        selectQuery: String,
        rowId: Int,
        waitTime: Int?,
        shouldCommit: Boolean?
    ): Connection {
        log("Connected to DB", transactionName)

        conn.autoCommit = false

        conn.prepareStatement(selectQuery).use { stmt ->
            stmt.setInt(1, rowId)
            log("Acquiring lock", transactionName)
            stmt.executeQuery().use { rs ->
                log("Acquired lock", transactionName)
                sleep(waitTime)
                logResults(rs)
            }
        }

        if (shouldCommit!!) conn.commit()
        return conn
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
        when (color) {
            "red" -> println("\u001B[31m$transactionName :: $message\u001B[0m")
            "green" -> println("\u001B[32m$transactionName :: $message\u001B[0m")
            else -> println("$transactionName :: $message")
        }
    }
}
