package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import java.sql
import org.apache.spark.rdd.RDD

case class DBTable(
    db: String, table: String, fields: Seq[String],
    key: String, minKey: Long, maxKey: Long) extends RowInput {
  assert(fields.contains(key), s"$key not found in $fields")

  def lines(rc: RuntimeContext): RDD[Seq[String]] = {
    val fieldsStr = fields.mkString(", ")
    new org.apache.spark.rdd.JdbcRDD(
      rc.sparkContext,
      () => sql.DriverManager.getConnection("jdbc:" + db),
      s"SELECT $fieldsStr FROM $table WHERE ? <= $key AND $key <= ?",
      minKey, maxKey, rc.defaultPartitioner.numPartitions,
      row => fields.map(field => row.getString(field))
    )
  }
}
