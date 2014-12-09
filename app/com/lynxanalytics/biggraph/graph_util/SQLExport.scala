package com.lynxanalytics.biggraph.graph_util

import java.sql
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

// A few methods are extracted to the companion object to avoid trying to
// serialize the case class.
private object SQLExport {
  def quote(s: String) = '"' + StringEscapeUtils.escapeJava(s) + '"'
  def addRDDs(base: SortedRDD[ID, Seq[String]], rdds: Seq[SortedRDD[ID, String]]) = {
    rdds.foldLeft(base) { (seqs, rdd) =>
      seqs
        .sortedLeftOuterJoin(rdd)
        .mapValues { case (seq, opt) => seq :+ opt.getOrElse("NULL") }
    }
  }

  def makeInserts(table: String, rdd: RDD[Seq[String]]) = {
    rdd.mapPartitions { it =>
      val lines = it.map(seq => " (" + seq.mkString(", ") + ")").mkString(",\n") + ";\n"
      Iterator(s"INSERT INTO $table VALUES\n" + lines)
    }
  }

  def execute(db: String, update: String) = {
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate(update);
    connection.close()
  }
}
import SQLExport._
case class SQLExport(
    table: String,
    vertexSet: VertexSet,
    attributes: Map[String, Attribute[_]])(implicit dataManager: DataManager) {

  private def attrsAs[T: TypeTag] = {
    attributes.filter { case (n, a) => a.is[T] }.mapValues(_.runtimeSafeCast[T])
  }
  private val doubles = attrsAs[Double]
  private val strings = attrsAs[String]
  for ((n, a) <- (attributes -- doubles.keys -- strings.keys)) {
    assert(false, s"Attribute '$n' is of an unsupported type: ${a.typeTag}")
  }
  private val doubleNames = doubles.keys.toSeq.sorted
  private val stringNames = strings.keys.toSeq.sorted
  private val columns = (
    doubleNames.map(_ + " DOUBLE PRECISION") ++
    stringNames.map(_ + " TEXT")).mkString(", ")

  private val doubleValues = addRDDs(
    vertexSet.rdd.mapValues(_ => Seq[String]()),
    doubleNames.map(n => doubles(n).rdd.mapValues(_.toString)))
  private val allValues = addRDDs(
    doubleValues,
    stringNames.map(n => strings(n).rdd.mapValues(quote(_))))

  val creation = s"""
      DROP TABLE IF EXISTS $table;
      CREATE TABLE $table ($columns);
      """

  val inserts = makeInserts(table, allValues.values)

  def insertInto(db: String) = {
    // Create the table from the driver.
    execute(db, creation)
    // Running the data updates from parallel tasks.
    inserts.foreach(execute(db, _))
  }

  def saveAs(filename: Filename) = {
    (filename / "schema").createFromStrings(creation)
    (filename / "data").saveAsTextFile(inserts)
  }
}
