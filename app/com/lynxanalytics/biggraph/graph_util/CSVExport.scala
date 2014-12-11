package com.lynxanalytics.biggraph.graph_util

import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

case class CSVData(val header: Seq[String],
                   val data: rdd.RDD[Seq[String]]) {
  override def toString: String =
    CSVData.lineToString(header) + data.map(CSVData.lineToString(_)).collect.mkString

  def toSortedString: String =
    CSVData.lineToString(header) + data.map(CSVData.lineToString(_)).collect.sorted.mkString

  def toStringRDD: rdd.RDD[String] = data.map(CSVData.lineToStringNoNewLine(_))

  def saveDataToDir(path: Filename) = path.saveAsTextFile(toStringRDD)

  def saveToDir(path: Filename) = {
    (path / "header").createFromStrings(CSVData.lineToString(header))
    saveDataToDir(path / "data")
  }
}
object CSVData {
  def lineToStringNoNewLine(line: Seq[String]): String = line.mkString(",")
  def lineToString(line: Seq[String]): String = lineToStringNoNewLine(line) + "\n"
}

object CSVExport {
  def exportVertexAttributes(attributes: Seq[Attribute[_]],
                             attributeLabels: Seq[String])(implicit dataManager: DataManager): CSVData = {
    assert(attributes.size > 0)
    assert(attributes.size == attributeLabels.size)
    val vertexSet = attributes.head.vertexSet
    assert(attributes.forall(_.vertexSet == vertexSet))
    val indexedVertexIds = vertexSet.rdd.mapValues(_ => Seq[String]())

    CSVData(
      attributeLabels.map(quoteString),
      attachAttributeData(indexedVertexIds, attributes).values)
  }

  def exportEdgeAttributes(
    edgeBundle: EdgeBundle,
    attributes: Seq[Attribute[_]],
    attributeLabels: Seq[String])(implicit dataManager: DataManager): CSVData = {

    assert(attributes.size == attributeLabels.size)
    assert(attributes.forall(_.vertexSet == edgeBundle.asVertexSet))
    val indexedEdges = edgeBundle.rdd.mapValues {
      edge => Seq(edge.src.toString, edge.dst.toString)
    }

    CSVData(
      ("srcVertexId" +: "dstVertexId" +: attributeLabels).map(quoteString),
      attachAttributeData(indexedEdges, attributes).values)
  }

  private def attachAttributeData(
    keyData: rdd.RDD[(ID, Seq[String])],
    attributes: Seq[Attribute[_]])(implicit dataManager: DataManager): rdd.RDD[(ID, Seq[String])] = {

    var indexedData = keyData
    for (attribute <- attributes) {
      indexedData = indexedData
        .leftOuterJoin(stringRDDFromAttribute(attribute))
        .mapValues {
          case (prev, current) =>
            current match {
              case Some(value) => prev :+ value
              case None => prev :+ ""
            }
        }
    }
    indexedData
  }

  private def stringRDDFromAttribute[T: ClassTag](
    attribute: Attribute[T])(implicit dataManager: DataManager): rdd.RDD[(ID, String)] = {
    implicit val tagForT = attribute.typeTag
    val op = toCSVStringOperation[T]
    attribute.rdd.mapValues(op)
  }

  private def toCSVStringOperation[T: TypeTag]: T => String = {
    if (typeOf[T] =:= typeOf[String]) {
      stringValue => quoteString(stringValue.asInstanceOf[String])
    } else {
      objectValue => objectValue.toString
    }
  }

  private def quoteString(s: String) = "\"" + StringEscapeUtils.escapeJava(s) + "\""

}
