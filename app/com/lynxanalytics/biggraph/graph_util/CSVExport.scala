package com.lynxanalytics.biggraph.graph_util

import java.io.IOException
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

case class CSVData(val header: Seq[String],
                   val data: rdd.RDD[Seq[String]]) {
  override def toString: String =
    CSVData.lineToString(header) + data.map(CSVData.lineToString(_)).collect.mkString

  def toStringRDD: rdd.RDD[String] = data.map(CSVData.lineToStringNoNewLine(_))

  def saveDataToDir(path: String) = toStringRDD.saveAsTextFile(path)
}
object CSVData {
  def lineToStringNoNewLine(line: Seq[String]): String = line.mkString(",")
  def lineToString(line: Seq[String]): String = lineToStringNoNewLine(line) + "\n"
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

  def exportToDirectory(graphData: GraphData,
                        directoryPath: String): Unit = {
    val directoryHadoopPath = new hadoop.fs.Path(directoryPath)
    val fs = directoryHadoopPath.getFileSystem(new hadoop.conf.Configuration())
    if (fs.exists(directoryHadoopPath)) {
      throw new IOException("Directory already exists")
    }
    fs.mkdirs(directoryHadoopPath)

    val vertexCsvData = exportVertices(graphData)
    writeStringToFile(
      fs,
      new hadoop.fs.Path(directoryHadoopPath, "vertex-header"),
      CSVData.lineToString(vertexCsvData.header))
    vertexCsvData.saveDataToDir(directoryPath + "/vertex-data")

    val edgeCsvData = exportEdges(graphData)
    writeStringToFile(
      fs,
      new hadoop.fs.Path(directoryHadoopPath, "edge-header"),
      CSVData.lineToString(edgeCsvData.header))
    edgeCsvData.saveDataToDir(directoryPath + "/edge-data")
  }

  private def quoteString(s: String) = "\"" + StringEscapeUtils.escapeJava(s) + "\""

  private object CSVCellConverter extends TypeDependentOperation[String] {
    def getReaderForIndex[S: TypeTag](idx: AttributeReadIndex[S]): AttributeReader[String] = {
      new ConvertedAttributeReader[S, String](
        idx,
        if (typeOf[S] =:= typeOf[String]) {
          stringValue => quoteString(stringValue.asInstanceOf[String])
        } else if (typeOf[S] =:= typeOf[Array[Long]]) {
          arrayValue => arrayValue.asInstanceOf[Array[Long]].mkString(";")
        } else {
          objectValue => objectValue.toString
        })
    }
  }

  private def writeStringToFile(fs: hadoop.fs.FileSystem,
                                path: hadoop.fs.Path,
                                contents: String): Unit = {
    val stream = fs.create(path)
    stream.write(contents.getBytes("UTF-8"))
    stream.close()
  }
}
