// A persistent key-value storage interface and implementation(s).
package com.lynxanalytics.biggraph.graph_api

trait KeyValueStore {
  def clear: Unit
  def get(key: String): Option[String]
  def put(key: String, value: String): Unit
  def scan(prefix: String): Iterator[(String, String)]
  def transaction[T](fn: => T): T // Can be nested.
}
object KeyValueStore {
  def apply(file: String) = new SQLiteKeyValueStore(file)
}

class SQLiteKeyValueStore(file: String) extends KeyValueStore {
  import anorm.SqlStringInterpolation
  import anorm.SqlParser.{ flatten, str }
  implicit val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + file)
  SQL"CREATE TABLE IF NOT EXISTS tags (key TEXT PRIMARY KEY, value TEXT)"
    .executeUpdate

  def clear: Unit = synchronized {
    SQL"DROP TABLE tags"
      .executeUpdate
    SQL"CREATE TABLE IF NOT EXISTS tags (key TEXT PRIMARY KEY, value TEXT)"
      .executeUpdate
  }

  def get(key: String): Option[String] = synchronized {
    SQL"SELECT value FROM tags WHERE key = $key"
      .as(str("value").singleOpt)
  }

  def put(key: String, value: String): Unit = synchronized {
    SQL"INSERT OR REPLACE INTO tags VALUES ($key, $value)"
      .executeUpdate
  }

  def scan(prefix: String): Iterator[(String, String)] = synchronized {
    val z = '\uffff' // Highest character code.
    SQL"SELECT key, value FROM tags WHERE key BETWEEN $prefix AND ${prefix + z}"
      .as((str("key") ~ str("value")).*).map(flatten).iterator
  }

  def transaction[T](fn: => T): T = synchronized {
    val ac = connection.getAutoCommit
    connection.setAutoCommit(false)
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
}
