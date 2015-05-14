// Export to SQL databases through JDBC.
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
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object SQLExport {
  def quoteIdentifier(s: String) = '"' + s.replaceAll("\"", "\"\"") + '"'
  private def quoteData(s: String) = "'" + StringEscapeUtils.escapeSql(s) + "'"
  private def addRDDs(base: SortedRDD[ID, Seq[String]], rdds: Seq[SortedRDD[ID, String]]) = {
    rdds.foldLeft(base) { (seqs, rdd) =>
      seqs
        .sortedLeftOuterJoin(rdd)
        .mapValues { case (seq, opt) => seq :+ opt.getOrElse("NULL") }
    }
  }

  private def makeInserts(quotedTable: String, rdd: RDD[Seq[String]]) = {
    rdd.mapPartitions { it =>
      it.map(seq =>
        s"INSERT INTO $quotedTable VALUES (" + seq.mkString(", ") + ");")
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
    (typeOf[String], "VARCHAR(512)",
      rdd => rdd.asInstanceOf[AttributeRDD[String]].mapValues(quoteData(_))),
    (typeOf[Long], "BIGINT",
      rdd => rdd.asInstanceOf[AttributeRDD[Long]].mapValues(_.toString)))

  private case class SQLColumn(name: String, sqlType: String, stringRDD: SortedRDD[ID, String])

  private def sqlAttribute[T](name: String, attr: Attribute[T])(implicit dm: DataManager) = {
    val opt = supportedTypes.find(line => line._1 =:= attr.typeTag.tpe)
    assert(opt.nonEmpty, s"Attribute '$name' is of an unsupported type: ${attr.typeTag}")
    val (tpe, sqlType, toStringFn) = opt.get
    SQLColumn(name, sqlType, toStringFn(attr.rdd))
  }

  def apply(
    table: String,
    vertexSet: VertexSet,
    attributes: Map[String, Attribute[_]])(implicit dataManager: DataManager): SQLExport = {
    for ((name, attr) <- attributes) {
      assert(attr.vertexSet == vertexSet, s"Attribute $name is not for vertex set $vertexSet")
    }
    new SQLExport(table, vertexSet.rdd, attributes.toSeq.sortBy(_._1).map {
      case (name, attr) => sqlAttribute(name, attr)
    })
  }

  def apply(
    table: String,
    edgeBundle: EdgeBundle,
    attributes: Map[String, Attribute[_]],
    srcColumnName: String = "srcVertexId",
    dstColumnName: String = "dstVertexId")(implicit dataManager: DataManager): SQLExport = {
    for ((name, attr) <- attributes) {
      assert(attr.vertexSet == edgeBundle.idSet,
        s"Attribute $name is not for edge bundle $edgeBundle")
    }
    new SQLExport(table, edgeBundle.idSet.rdd, Seq(
      SQLColumn(srcColumnName, "BIGINT", edgeBundle.rdd.mapValues(_.src.toString)),
      SQLColumn(dstColumnName, "BIGINT", edgeBundle.rdd.mapValues(_.dst.toString))
    ) ++ attributes.toSeq.sortBy(_._1).map { case (name, attr) => sqlAttribute(name, attr) })
  }
}
import SQLExport._
class SQLExport private (
    table: String,
    vertexSet: VertexSetRDD,
    sqls: Seq[SQLColumn]) {

  private val columns = sqls
    .map(col => s"${quoteIdentifier(col.name)} ${col.sqlType}")
    .mkString(", ")
  private val values = addRDDs(
    vertexSet.mapValues(_ => Seq[String]()),
    sqls.map(_.stringRDD)).values

  val quotedTable = quoteIdentifier(table);
  val deletion = s"DROP TABLE IF EXISTS $quotedTable;\n"
  val creation = s"CREATE TABLE $quotedTable ($columns);\n"
  log.info(s"Executing statements: $deletion $creation")
  val inserts = makeInserts(quotedTable, values)

  def insertInto(db: String, delete: Boolean) = {
    // Create the table from the driver.
    if (delete) execute(db, deletion + creation)
    else execute(db, creation)
    // Running the data updates from parallel tasks.
    inserts.foreach(execute(db, _))
  }

  def saveAs(filename: HadoopFile) = {
    (filename / "header").createFromStrings(deletion + creation)
    (filename / "data").saveAsTextFile(inserts)
  }
}
