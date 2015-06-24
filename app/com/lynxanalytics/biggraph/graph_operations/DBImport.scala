// DBTable is a RowInput that can be used with import operations to import via JDBC.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.SQLExport.quoteIdentifier
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
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

  val quotedTable = quoteIdentifier(table)
  val quotedKey = quoteIdentifier(key)

  override def toJson = Json.obj(
    "db" -> db,
    "table" -> table,
    "fields" -> fields,
    "key" -> key)

  case class Stats(table: String)(implicit val connection: sql.Connection) {
    val (minKey, maxKey) = {
      val query = s"SELECT MIN($quotedKey) AS min, MAX($quotedKey) AS max FROM $quotedTable"
      log.info(s"Executing query: $query")
      val q = SQL(query)
      q().map(r => (r[Long]("min"), r[Long]("max"))).head
    }
    val count = {
      val query = s"SELECT COUNT(*) AS count FROM $quotedTable"
      log.info(s"Executing query: $query")
      val q = SQL(query)
      q().map(r => r[Long]("count")).head
    }
  }

  def lines(rc: RuntimeContext): SortedRDD[ID, Seq[String]] = {
    val fieldsStr = fields.map(quoteIdentifier(_)).mkString(", ")
    val stats = {
      val connection = sql.DriverManager.getConnection("jdbc:" + db)
      try Stats(table)(connection)
      finally connection.close()
    }
    // Have at most 100,000 rows per partition.
    val numPartitions = rc.partitionerForNBytes(stats.count * fields.size * 30).numPartitions

    val query = s"SELECT $fieldsStr FROM $quotedTable WHERE ? <= $quotedKey AND $quotedKey <= ?"
    log.info(s"Executing query: $query")
    new org.apache.spark.rdd.JdbcRDD(
      rc.sparkContext,
      () => sql.DriverManager.getConnection("jdbc:" + db),
      query,
      stats.minKey, stats.maxKey, numPartitions,
      row => fields.map(field => row.getString(field))
    ).randomNumbered(numPartitions)
  }

  val mayHaveNulls = true
}
