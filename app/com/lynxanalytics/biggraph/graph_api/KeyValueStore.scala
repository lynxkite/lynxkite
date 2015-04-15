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
}

class SQLiteKeyValueStore(file: String) extends KeyValueStore {
  import anorm.SqlStringInterpolation
  import anorm.SqlParser.{ flatten, str }
  new java.io.File(file).getParentFile.mkdirs // SQLite cannot create the directory.
  implicit val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + file)
  SQL"CREATE TABLE IF NOT EXISTS tags (key TEXT PRIMARY KEY, value TEXT)"
    .executeUpdate
  val Z = '\uffff' // Highest character code.

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

  def delete(key: String): Unit = synchronized {
    SQL"DELETE FROM tags WHERE key = $key"
      .executeUpdate
  }

  def put(key: String, value: String): Unit = synchronized {
    SQL"INSERT OR REPLACE INTO tags VALUES ($key, $value)"
      .executeUpdate
  }

  def scan(prefix: String): Iterable[(String, String)] = synchronized {
    SQL"SELECT key, value FROM tags WHERE key BETWEEN $prefix AND ${prefix + Z}"
      .as((str("key") ~ str("value")).*).map(flatten)
  }

  def deletePrefix(prefix: String): Unit = synchronized {
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
}
