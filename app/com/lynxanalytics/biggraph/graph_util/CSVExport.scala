package com.lynxanalytics.biggraph.graph_util

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

case class CSVData(val header: Seq[String],
              val data: rdd.RDD[Seq[String]]) {
  override def toString: String =
    lineToString(header) + data.map(lineToString(_)).collect.mkString

  private def lineToString(line: Seq[String]): String = line.mkString(",") + "\n"
}

object CSVExport {
  def exportVertices(graphData: GraphData): CSVData = {
    val readers = graphData.bigGraph.vertexAttributes.getReadersForOperation(CSVCellConverter)
    CSVData(
      ("vertexId" +: graphData.bigGraph.vertexAttributes.attributeSeq).map(quoteString),
      graphData.vertices.map {
        case (id, attr) => id.toString +: readers.map(_.readFrom(attr))
      })
  }
  def exportEdges(graphData: GraphData): CSVData = {
    val readers = graphData.bigGraph.edgeAttributes.getReadersForOperation(CSVCellConverter)
    CSVData(
      ("srcVertexId" +: "dstVertexId" +: graphData.bigGraph.edgeAttributes.attributeSeq)
        .map(quoteString),
      graphData.edges.map {
        case graphx.Edge(srcId, dstId, attr) =>
          srcId.toString +: dstId.toString +: readers.map(_.readFrom(attr))
      })
  }

  private def quoteString(s: String) = "\"" + StringEscapeUtils.escapeJava(s) + "\""

  private object CSVCellConverter extends TypeDependentOperation[String] {
    def getReaderForIndex[S: TypeTag](idx: AttributeReadIndex[S]): AttributeReader[String] = {
      if (typeOf[S] =:= typeOf[String]) {
        new ConvertedAttributeReader[S, String](
          idx, value => quoteString(value.asInstanceOf[String]))
      } else {
        new ConvertedAttributeReader[S, String](idx, _.toString)
      }
    }
  }
}
