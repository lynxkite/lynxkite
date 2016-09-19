// DBTable is a RowInput that can be used with import operations to import via JDBC.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_util.TableStats
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import java.sql

@deprecated("Replaced by table-based importing.", "1.7.0")
object DBTable extends FromJson[DBTable] {
  def fromJson(j: JsValue) = DBTable(
    (j \ "db").as[String],
    (j \ "table").as[String],
    (j \ "fields").as[Seq[String]],
    (j \ "key").as[String])
  // SI-9650
  def apply(db: String, table: String, fields: Seq[String], key: String) =
    new DBTable(db, table, fields, key)
}
@deprecated("Replaced by table-based importing.", "1.7.0")
class DBTable(
    val db: String, val table: String, val fields: Seq[String],
    val key: String) extends RowInput with Serializable {
  override def equals(o: Any) = {
    o.isInstanceOf[DBTable] && {
      val other = o.asInstanceOf[DBTable]
      other.db == db && other.table == table && other.fields == fields && other.key == key
    }
  }
  assert(fields.contains(key), s"$key not found in $fields")

  override def toJson = Json.obj(
    "db" -> db,
    "table" -> table,
    "fields" -> fields,
    "key" -> key)

  // Deprecated functionality has been removed.
  def lines(rc: RuntimeContext): UniqueSortedRDD[ID, Seq[String]] = ???

  val mayHaveNulls = true
}
