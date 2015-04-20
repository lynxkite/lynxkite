// A persistent key-value storage interface and implementation(s).
package com.lynxanalytics.biggraph.graph_api

trait KeyValueStore {
  def clear: Unit
  def get(key: String): Option[String]
  def delete(key: String): Unit
  def put(key: String, value: String): Unit
  def scan(prefix: String): Iterable[(String, String)]
  def deletePrefix(prefix: String): Unit
  def transaction[T](fn: => T): T // Can be nested.
  def writesCanBeIgnored[T](fn: => T): T // May ignore writes from "fn".
}

case class SQLiteKeyValueStore(file: String) extends KeyValueStore {
  import anorm.SqlStringInterpolation
  import anorm.SqlParser.{ flatten, str }
  new java.io.File(file).getParentFile.mkdirs // SQLite cannot create the directory.
  implicit val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + file)
  private val Z = '\uffff' // Highest character code.

  private def createTableIfNotExists: Unit = {
    SQL"CREATE TABLE IF NOT EXISTS tags (key TEXT PRIMARY KEY, value TEXT)"
      .executeUpdate
  }
  createTableIfNotExists // Make sure the table exists.

  def clear: Unit = if (doWrites) synchronized {
    SQL"DROP TABLE tags".executeUpdate
    createTableIfNotExists
  }

  def get(key: String): Option[String] = synchronized {
    SQL"SELECT value FROM tags WHERE key = $key"
      .as(str("value").singleOpt)
  }

  def delete(key: String): Unit = if (doWrites) synchronized {
    SQL"DELETE FROM tags WHERE key = $key"
      .executeUpdate
  }

  def put(key: String, value: String): Unit = if (doWrites) synchronized {
    SQL"INSERT OR REPLACE INTO tags VALUES ($key, $value)"
      .executeUpdate
  }

  def scan(prefix: String): Iterable[(String, String)] = synchronized {
    SQL"SELECT key, value FROM tags WHERE key BETWEEN $prefix AND ${prefix + Z}"
      .as((str("key") ~ str("value")).*).map(flatten)
  }

  def deletePrefix(prefix: String): Unit = if (doWrites) synchronized {
    SQL"DELETE FROM tags WHERE key BETWEEN $prefix AND ${prefix + Z}"
      .executeUpdate
  }

  def transaction[T](fn: => T): T = synchronized {
    val ac = connection.getAutoCommit
    connection.setAutoCommit(false)
    // The SQLite JDBC driver does not support savepoints, but SQLite itself does.
    // So we create them manually.
    SQL"SAVEPOINT 'X'".executeUpdate
    val t = util.Try(fn)
    if (t.isSuccess) {
      SQL"RELEASE 'X'".executeUpdate
    } else {
      SQL"ROLLBACK TO 'X'".executeUpdate
    }
    connection.setAutoCommit(ac)
    t.get
  }

  private var ignoreWrites = 0
  private def doWrites = ignoreWrites == 0
  def writesCanBeIgnored[T](fn: => T): T = {
    ignoreWrites += 1
    try { fn }
    finally { ignoreWrites -= 1 }
  }
}

case class JsonKeyValueStore(file: String) extends KeyValueStore {
  import java.io.File
  import org.apache.commons.io.FileUtils
  import play.api.libs.json.Json

  private val raw = FileUtils.readFileToString(new File(file), "utf8")
  private val map = Json.parse(raw).as[Map[String, String]]

  def get(key: String): Option[String] = map.get(key)
  def scan(prefix: String): Iterable[(String, String)] = map.filter(_._1.startsWith(prefix))

  // This is a read-only implementation, used for backward-compatibility.
  def clear: Unit = ???
  def delete(key: String): Unit = ???
  def put(key: String, value: String): Unit = ???
  def deletePrefix(prefix: String): Unit = ???
  def transaction[T](fn: => T): T = ???
  def writesCanBeIgnored[T](fn: => T): T = fn
}
