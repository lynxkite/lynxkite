package com.lynxanalytics.biggraph.graph_util

import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

case class CSVData(val header: Seq[String],
                   val data: rdd.RDD[Seq[String]]) {
  override def toString: String =
    CSVData.lineToString(header) + data.map(CSVData.lineToString(_)).collect.mkString

  def toStringRDD: rdd.RDD[String] = data.map(CSVData.lineToStringNoNewLine(_))

  def saveDataToDir(path: Filename) = path.saveAsTextFile(toStringRDD)

  def saveToDir(path: Filename) = {
    path.addPathElement("header").createFromStrings(CSVData.lineToString(header))
    saveDataToDir(path.addPathElement("data"))
  }
}
object CSVData {
  def lineToStringNoNewLine(line: Seq[String]): String = line.mkString(",")
  def lineToString(line: Seq[String]): String = lineToStringNoNewLine(line) + "\n"
}

object CSVExport {
  def exportVertexAttributes(attributes: Seq[VertexAttribute[_]],
                             attributeLabels: Seq[String],
                             dataManager: DataManager): CSVData = {
    assert(attributes.size > 0)
    assert(attributes.size == attributeLabels.size)
    val vertexSet = attributes.head.vertexSet
    assert(attributes.forall(_.vertexSet == vertexSet))
    val indexedVertexIds: rdd.RDD[(ID, Seq[String])] =
      dataManager.get(vertexSet).rdd.mapPartitions(it =>
        it.map {
          case (id, _) =>
            (id, Vector(id.toString))
        },
        preservesPartitioning = true)

    CSVData(
      ("vertexId" +: attributeLabels).map(quoteString),
      attachAttributeData(indexedVertexIds, attributes, dataManager).values)
  }

  def exportEdgeAttributes(attributes: Seq[EdgeAttribute[_]],
                           attributeLabels: Seq[String],
                           dataManager: DataManager): CSVData = {
    assert(attributes.size > 0)
    assert(attributes.size == attributeLabels.size)
    val edgeBundle = attributes.head.edgeBundle
    assert(attributes.forall(_.edgeBundle == edgeBundle))
    val indexedEdges: rdd.RDD[(ID, Seq[String])] =
      dataManager.get(edgeBundle).rdd.mapPartitions(it =>
        it.map {
          case (id, edge) =>
            (id, Vector(id.toString, edge.src.toString, edge.dst.toString))
        },
        preservesPartitioning = true)

    CSVData(
      ("edgeId" +: "srcVertexId" +: "dstVertexId" +: attributeLabels).map(quoteString),
      attachAttributeData(indexedEdges, attributes, dataManager).values)
  }

  private def attachAttributeData(
    keyData: rdd.RDD[(ID, Seq[String])],
    attributes: Seq[Attribute[_]],
    dataManager: DataManager): rdd.RDD[(ID, Seq[String])] = {

    var indexedData = keyData
    for (attribute <- attributes) {
      indexedData = indexedData
        .leftOuterJoin(stringRDDFromAttribute(dataManager, attribute))
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
    dataManager: DataManager, attribute: Attribute[T]): rdd.RDD[(ID, String)] = {
    implicit val tagForT = attribute.typeTag
    val op = toCSVStringOperation[T]
    dataManager.get(attribute).rdd.mapValues(op)
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
