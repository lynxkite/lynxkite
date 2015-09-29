// Export data to CSV files.
package com.lynxanalytics.biggraph.graph_util

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.rdd
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.language.existentials

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class CSVData(val header: Seq[String],
                   val data: rdd.RDD[Seq[String]]) {
  override def toString: String =
    CSVData.lineToString(header) + data.map(CSVData.lineToString(_)).collect.mkString

  def toStringRDD: rdd.RDD[String] = data.map(CSVData.lineToStringNoNewLine(_))

  def saveDataToDir(path: HadoopFile) = path.saveAsTextFile(toStringRDD)

  def saveToDir(path: HadoopFile) = {
    (path / "header").createFromStrings(CSVData.lineToString(header))
    saveDataToDir(path / "data")
  }
}
object CSVData {
  def lineToStringNoNewLine(line: Seq[String]): String = line.mkString(",")
  def lineToString(line: Seq[String]): String = lineToStringNoNewLine(line) + "\n"
}

object CSVExport {
  def exportVertexAttributes(
    vertexSet: VertexSet,
    attributes: Map[String, Attribute[_]])(implicit dataManager: DataManager): CSVData = {
    assert(attributes.size > 0, "At least one attribute must be selected for export.")
    for ((name, attr) <- attributes) {
      assert(attr.vertexSet == vertexSet, s"Incorrect vertex set for attribute $name.")
    }
    val indexedVertexIds = vertexSet.rdd.mapValues(_ => Seq[String]())
    val (names, attrs) = attributes.toSeq.sortBy(_._1).unzip
    CSVData(
      names.map(quoteString(_)),
      attachAttributeData(indexedVertexIds, attrs).values)
  }

  private def computeIndexedEdges(
    edgeBundle: EdgeBundle,
    srcAttr: Attribute[_],
    dstAttr: Attribute[_])(implicit dataManager: DataManager): SortedRDD[ID, Seq[String]] = {
    val v1 = edgeBundle.rdd.map { // (src, (dst, id)
      case (id, edge) => (edge.src, (edge.dst, id))
    }
    val v2 = srcAttr.rdd.join(v1) // (src, (attr_src, (dst, id)))
    val v3 = v2.map {
      case (src, (attr_src, (dst, id))) => (dst, (src, attr_src, id))
    }
    val v4 = dstAttr.rdd.join(v3) // (dst, (attr_dst, (src, attr_src, id)))
    val v5 = v4.map {
      case (dst, (attr_dst, (src, attr_src, id))) => (id, (attr_src, attr_dst))
    }
    val v6 = v5.mapValues {
      attrs => Seq(attrs._1.toString, attrs._2.toString)
    }
    v6.toSortedRDD(edgeBundle.rdd.partitioner.get)
  }

  def exportEdgeAttributes(
    edgeBundle: EdgeBundle,
    attributes: Map[String, Attribute[_]],
    srcAttr: Attribute[_],
    dstAttr: Attribute[_],
    srcColumnName: String = "srcVertexId",
    dstColumnName: String = "dstVertexId")(implicit dataManager: DataManager): CSVData = {
    for ((name, attr) <- attributes) {
      assert(attr.vertexSet == edgeBundle.idSet,
        s"Incorrect vertex set for attribute $name.")
    }
    val indexedEdges = computeIndexedEdges(edgeBundle, srcAttr, dstAttr)

    val (names, attrs) = attributes.toList.sortBy(_._1).unzip
    CSVData(
      (srcColumnName :: dstColumnName :: names).map(quoteString),
      attachAttributeData(indexedEdges, attrs).values)
  }

  private def addRDDs(base: SortedRDD[ID, Seq[String]], rdds: Seq[SortedRDD[ID, String]]) = {
    rdds.foldLeft(base) { (seqs, rdd) =>
      seqs
        .sortedLeftOuterJoin(rdd)
        .mapValues { case (seq, opt) => seq :+ opt.getOrElse("") }
    }
  }

  private def attachAttributeData(
    base: SortedRDD[ID, Seq[String]],
    attributes: Seq[Attribute[_]])(implicit dataManager: DataManager) = {
    addRDDs(base, attributes.map(stringRDDFromAttribute(_)))
  }

  private def stringRDDFromAttribute[T: ClassTag](
    attribute: Attribute[T])(implicit dataManager: DataManager): SortedRDD[ID, String] = {
    implicit val tagForT = attribute.typeTag
    val op = toCSVStringOperation[T]
    attribute.rdd.mapValues(op)
  }

  private def toCSVStringOperation[T: TypeTag]: T => String = {
    if (typeOf[T] =:= typeOf[String]) {
      stringValue => quoteString(stringValue.asInstanceOf[String])
    } else if (typeOf[T] <:< typeOf[Iterable[Any]]) {
      val insideTT = TypeTagUtil.typeArgs(typeTag[T]).head
      iterableQuoter(insideTT).asInstanceOf[T => String]
    } else {
      objectValue => objectValue.toString
    }
  }

  private def iterableQuoter[T: TypeTag]: Iterable[T] => String = {
    val insideFunc = toCSVStringOperation[T]
    it => it.map(insideFunc).mkString(";")
  }

  private def quoteString(s: String) = "\"" + StringEscapeUtils.escapeJava(s) + "\""

}
