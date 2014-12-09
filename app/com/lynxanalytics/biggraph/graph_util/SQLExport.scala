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
object SQLExport {
  private def quote(s: String) = '"' + StringEscapeUtils.escapeJava(s) + '"'
  private def addRDDs(base: SortedRDD[ID, Seq[String]], rdds: Seq[SortedRDD[ID, String]]) = {
    rdds.foldLeft(base) { (seqs, rdd) =>
      seqs
        .sortedLeftOuterJoin(rdd)
        .mapValues { case (seq, opt) => seq :+ opt.getOrElse("NULL") }
    }
  }

  private def makeInserts(table: String, rdd: RDD[Seq[String]]) = {
    rdd.mapPartitions { it =>
      val lines = it.map(seq => " (" + seq.mkString(", ") + ")").mkString(",\n") + ";\n"
      Iterator(s"INSERT INTO $table VALUES\n" + lines)
    }
  }

  private def execute(db: String, update: String) = {
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate(update);
    connection.close()
  }

  private val supportedTypes: Seq[(Type, String, AttributeRDD[_] => AttributeRDD[String])] = Seq(
    (typeOf[Double], "DOUBLE PRECISION",
      rdd => rdd.asInstanceOf[AttributeRDD[Double]].mapValues(_.toString)),
    (typeOf[String], "TEXT",
      rdd => rdd.asInstanceOf[AttributeRDD[String]].mapValues(quote(_))),
    (typeOf[Long], "BIGINT",
      rdd => rdd.asInstanceOf[AttributeRDD[Long]].mapValues(_.toString)))

  case class SQLAttribute(name: String, attr: Attribute[_])(implicit dm: DataManager) {
    val opt = supportedTypes.find(line => line._1 =:= attr.typeTag.tpe)
    assert(opt.nonEmpty, s"Attribute '$name' is of an unsupported type: ${attr.typeTag}")
    val (tpe, sqlType, toStringFn) = opt.get
    val asString = toStringFn(attr.rdd)
  }
}
import SQLExport._
case class SQLExport(
    table: String,
    vertexSet: VertexSet,
    attributes: Map[String, Attribute[_]])(implicit dataManager: DataManager) {

  private val names = attributes.keys.toSeq.sorted
  private val sqls = names.map(name => SQLAttribute(name, attributes(name)))
  private val columns = sqls.map(attr => attr.name + " " + attr.sqlType).mkString(", ")
  private val values = addRDDs(
    vertexSet.rdd.mapValues(_ => Seq[String]()),
    sqls.map(_.asString)).values

  val creation = s"""
      DROP TABLE IF EXISTS $table;
      CREATE TABLE $table ($columns);
      """

  val inserts = makeInserts(table, values)

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
