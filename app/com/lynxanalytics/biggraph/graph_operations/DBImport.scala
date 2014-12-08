package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import anorm.SQL
import java.sql
import org.apache.spark.rdd.RDD

case class DBTable(
    db: String, table: String, fields: Seq[String],
    key: String) extends RowInput {
  assert(fields.contains(key), s"$key not found in $fields")

  def lines(rc: RuntimeContext): RDD[Seq[String]] = {
    val fieldsStr = fields.mkString(", ")
    val (minKey, maxKey) = {
      implicit val connection = sql.DriverManager.getConnection("jdbc:" + db)
      try {
        val q = SQL(s"SELECT MIN($key) AS min, MAX($key) AS max FROM $table")
        q().map(r => (r[Long]("min"), r[Long]("max"))).head
      } finally connection.close()
    }

    new org.apache.spark.rdd.JdbcRDD(
      rc.sparkContext,
      () => sql.DriverManager.getConnection("jdbc:" + db),
      s"SELECT $fieldsStr FROM $table WHERE ? <= $key AND $key <= ?",
      minKey, maxKey, rc.defaultPartitioner.numPartitions,
      row => fields.map(field => row.getString(field))
    )
  }
}
