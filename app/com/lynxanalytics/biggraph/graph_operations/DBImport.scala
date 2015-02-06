package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import anorm.SQL
import java.sql
import org.apache.spark.rdd.RDD

object DBTable extends FromJson[DBTable] {
  def fromJson(j: JsValue) = DBTable(
    (j \ "db").as[String],
    (j \ "table").as[String],
    (j \ "fields").as[Seq[String]],
    (j \ "key").as[String])
}
case class DBTable(
    db: String, table: String, fields: Seq[String],
    key: String) extends RowInput {
  assert(fields.contains(key), s"$key not found in $fields")

  override def toJson = Json.obj(
    "db" -> db,
    "table" -> table,
    "fields" -> fields,
    "key" -> key)

  case class Stats(table: String)(implicit val connection: sql.Connection) {
    val (minKey, maxKey) = {
      val q = SQL(s"SELECT MIN($key) AS min, MAX($key) AS max FROM $table")
      q().map(r => (r[Long]("min"), r[Long]("max"))).head
    }
    val count = {
      val q = SQL(s"SELECT COUNT(*) AS count FROM $table")
      q().map(r => r[Long]("count")).head
    }
  }

  def lines(rc: RuntimeContext): RDD[Seq[String]] = {
    val fieldsStr = fields.mkString(", ")
    val stats = {
      val connection = sql.DriverManager.getConnection("jdbc:" + db)
      try Stats(table)(connection)
      finally connection.close()
    }
    // Have at most 100,000 rows per partition.
    val numPartitions = rc.defaultPartitioner.numPartitions max (stats.count / 100000).toInt

    new org.apache.spark.rdd.JdbcRDD(
      rc.sparkContext,
      () => sql.DriverManager.getConnection("jdbc:" + db),
      s"SELECT $fieldsStr FROM $table WHERE ? <= $key AND $key <= ?",
      stats.minKey, stats.maxKey, numPartitions,
      row => fields.map(field => row.getString(field))
    )
  }
}
