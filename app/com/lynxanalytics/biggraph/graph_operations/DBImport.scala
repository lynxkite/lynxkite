// DBTable is a RowInput that can be used with import operations to import via JDBC.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_util.TableStats
import com.lynxanalytics.biggraph.graph_util.JDBCQuoting.quoteIdentifier
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import java.sql

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

  def lines(rc: RuntimeContext): UniqueSortedRDD[ID, Seq[String]] = {
    val fieldsStr = fields.map(quoteIdentifier(_)).mkString(", ")
    val stats = {
      val connection = sql.DriverManager.getConnection("jdbc:" + db)
      try TableStats(table, key)(connection)
      finally connection.close()
    }
    val numPartitions = rc.partitionerForNRows(stats.count).numPartitions

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
