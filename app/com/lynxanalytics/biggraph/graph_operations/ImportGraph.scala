package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api.attributes._
import com.lynxanalytics.biggraph.spark_util
import com.lynxanalytics.biggraph.bigGraphLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark
import org.apache.spark.graphx
import java.util.regex.Pattern
import scala.reflect.ClassTag

/*
 * For a sequence of raw input meta data strings a sequence of AttributeWriters
 * will be instantiated. These will format their given input string to the provided
 * type and write the data into a DenseAttribute object at a given index as a side effect.
 * The sequence usually corresponds to a line of input file, the data will result
 * one or two DenseAttribute objects (for vertex and/or edge attributes).
 */
trait AttributeWriter[T] {
  def apply(input: T, attributes: DenseAttributes): Unit
}
case class StringWriter(idx: AttributeWriteIndex[String]) extends AttributeWriter[String] {
  def apply(input: String, attributes: DenseAttributes): Unit = {
    val strippedInput = input.stripPrefix("\"").stripSuffix("\"")
    attributes.set(idx, strippedInput)
  }
}

trait MetaDataParser {
  def getSignature(): AttributeSignature
  def createWriters(signature: AttributeSignature): Seq[AttributeWriter[String]]
}
case class HeaderAsStringCSVParser(inputFile: Filename, delimiter: String) extends MetaDataParser {
  lazy val csvHeader = inputFile.reader.readLine
    .split(Pattern.quote(delimiter))
    .map(sigName => sigName.stripPrefix("\"").stripSuffix("\""))
  inputFile.close

  def getSignature(): AttributeSignature = csvHeader.foldLeft(AttributeSignature.empty) {
    case (sig, name) => sig.addAttribute[String](name).signature
  }

  def createWriters(signature: AttributeSignature): Seq[AttributeWriter[String]] = {
    csvHeader.map { sigName =>
      val idx = signature.writeIndex[String](sigName)
      StringWriter(idx)
    }
  }
}
case class DummyMetaParser() extends MetaDataParser {
  def getSignature(): AttributeSignature = AttributeSignature.empty
  def createWriters(signature: AttributeSignature): Seq[AttributeWriter[String]] = Seq()
}

trait RawDataParser {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]]
}
case class ConcatenateCSVsDataParser(inputFiles: Seq[Filename],
                                     delimiter: String,
                                     skipFirstRow: Boolean) extends RawDataParser {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]] = {
    val lines = sc.union(inputFiles.map(file => {
      if (skipFirstRow) {
        sc.textFile(file.filename).mapPartitionsWithIndex(
          (p: Int, lines: Iterator[String]) => if (p == 0) lines.drop(1) else lines,
          true)
      } else {
        sc.textFile(file.filename)
      }
    }))
    lines.map(_.split(Pattern.quote(delimiter)))
  }
}
case class DummyDataParser() extends RawDataParser {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]] = sc.parallelize(Seq())
}

/*
 * The GraphBuilder wires together the parsed meta and raw data into BigGraph graph format,
 * also handles reindexing the vertex ids if needed.
 */
trait GraphBuilder {
  def build(vertexDataSig: AttributeSignature,
            vertexData: RDD[DenseAttributes],
            edgeDataSig: AttributeSignature,
            edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD)
}
trait GraphIndexer {
  def indexVertices[A: ClassTag](vertexData: RDD[A]): RDD[(Long, A)] =
    spark_util.RDDUtils.fastNumbered(vertexData)

  def indexEdges[A: ClassTag](edgeData: RDD[DenseAttributes],
                              eSrcIdx: attributes.AttributeReadIndex[A],
                              eDstIdx: attributes.AttributeReadIndex[A],
                              vertexFieldToId: RDD[(A, Long)]): EdgeRDD = {
    val edgesBySource: RDD[(A, (A, DenseAttributes))] = edgeData.map(edgeAttributes =>
      edgeAttributes(eSrcIdx) -> (edgeAttributes(eDstIdx), edgeAttributes))
      .partitionBy(vertexFieldToId.partitioner.get)
    val edgesByDest: RDD[(A, (Long, DenseAttributes))] = edgesBySource.join(vertexFieldToId).map {
      case (src, ((dst, eAttr), srcId)) => dst -> (srcId, eAttr)
    }.partitionBy(vertexFieldToId.partitioner.get)
    edgesByDest.join(vertexFieldToId).map {
      case (dst, ((srcId, eAttr), dstId)) => graphx.Edge(srcId, dstId, eAttr)
    }
  }
}

case class NumberedIdFromVertexField(vertexIdFieldName: String,
                                     sourceEdgeFieldName: String,
                                     destEdgeFieldName: String) extends GraphBuilder with GraphIndexer {
  def build(vertexDataSig: AttributeSignature,
            vertexData: RDD[DenseAttributes],
            edgeDataSig: AttributeSignature,
            edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD) = {
    val vertices: VertexRDD = indexVertices(vertexData)
    val vIdx = vertexDataSig.readIndex[String](vertexIdFieldName)
    val vertexAttrPartitioner = new spark.HashPartitioner(edgeData.partitions.size)
    val vertexFieldToId = vertices.map {
      case (vertexId, vertexAttributes) => vertexAttributes(vIdx) -> vertexId
    }.partitionBy(vertexAttrPartitioner)
    val eSrcIdx = edgeDataSig.readIndex[String](sourceEdgeFieldName)
    val eDstIdx = edgeDataSig.readIndex[String](destEdgeFieldName)
    val edges: EdgeRDD = indexEdges[String](edgeData, eSrcIdx, eDstIdx, vertexFieldToId)
    (vertices, edges)
  }
}
case class IdFromEdgeFields(vertexIdFieldName: String,
                            sourceEdgeFieldName: String,
                            destEdgeFieldName: String) extends GraphBuilder with GraphIndexer {
  def build(vertexDataSig: AttributeSignature,
            vertexData: RDD[DenseAttributes],
            edgeDataSig: AttributeSignature,
            edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD) = {
    val newVertexSig = vertexDataSig.addAttribute[String](vertexIdFieldName).signature
    val maker = newVertexSig.maker
    val vIdx = newVertexSig.writeIndex[String](vertexIdFieldName)
    val eSrcIdx = edgeDataSig.readIndex[String](sourceEdgeFieldName)
    val eDstIdx = edgeDataSig.readIndex[String](destEdgeFieldName)
    val verticesData = edgeData.flatMap(edgeAttributes =>
      Seq(edgeAttributes(eSrcIdx), edgeAttributes(eDstIdx))).distinct
    val verticesFromEdges: RDD[(Long, (String, DenseAttributes))] =
      indexVertices(verticesData.map(id => (id, maker.make.set(vIdx, id))))
    val vertexAttrPartitioner = new spark.HashPartitioner(edgeData.partitions.size)
    val vertexFieldToId = verticesFromEdges.map {
      case (vertexId, (field, attr)) => (field, vertexId)
    }.partitionBy(vertexAttrPartitioner)
    val edges: EdgeRDD = indexEdges[String](edgeData, eSrcIdx, eDstIdx, vertexFieldToId)
    val vertices: VertexRDD = verticesFromEdges.map {
      case (vertexId, (field, attr)) => (vertexId, attr)
    }
    (vertices, edges)
  }
}

/*
 * The actual importers are composed of various Parser and Builder implementations.
 * These must extend the ImportGraph which provides the actual implementation for
 * wiring the components together.
 */
class ImportGraph(vertexMeta: MetaDataParser,
                  vertexData: RawDataParser,
                  edgeMeta: MetaDataParser,
                  edgeData: RawDataParser,
                  graphBuilder: GraphBuilder) extends GraphOperation {
  @transient lazy val vertexSignature = vertexMeta.getSignature
  @transient lazy val edgeSignature = edgeMeta.getSignature

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext

    val vertexWriters = vertexMeta.createWriters(vertexSignature)
    val edgeWriters = edgeMeta.createWriters(edgeSignature)
    val rawVertices = vertexData.getRawData(sc)
    val rawEdges = edgeData.getRawData(sc)
    val vertexDenseAttributes = rawToDenseAttributes(
      vertexSignature.maker, vertexWriters, rawVertices)
    val edgeDenseAttributes = rawToDenseAttributes(
      edgeSignature.maker, edgeWriters, rawEdges)
    val (vertices, edges) = graphBuilder.build(
      vertexSignature, vertexDenseAttributes, edgeSignature, edgeDenseAttributes)
    new SimpleGraphData(target, vertices, edges) // TODO(forevian): add support for optional triplets
  }

  // The vertex attribute signature of the graph resulting from this operation.
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = vertexSignature

  // The edge attribute signature of the graph resulting from this operation.
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = edgeSignature

  private def rawToDenseAttributes[T](attributesMaker: DenseAttributesMaker,
                                      writers: Seq[AttributeWriter[T]],
                                      inputData: RDD[Seq[T]]): RDD[DenseAttributes] = {
    inputData.flatMap { line =>
      if (line.size == writers.size) {
        val attributes = attributesMaker.make
        writers.zip(line).foreach {
          case (writer, data) => writer(data, attributes)
        }
        Some(attributes)
      } else {
        bigGraphLogger.info("Input line cannot be parsed: %s".format(line.toString))
        None
      }
    }
  }
}

case class CSVImport(vertexHeader: Filename,
                     vertexCSVs: Seq[Filename],
                     edgeHeader: Filename,
                     edgeCSVs: Seq[Filename],
                     vertexIdFieldName: String,
                     sourceEdgeFieldName: String,
                     destEdgeFieldName: String,
                     delimiter: String,
                     skipFirstRow: Boolean)
    extends ImportGraph(HeaderAsStringCSVParser(vertexHeader, delimiter),
      ConcatenateCSVsDataParser(vertexCSVs, delimiter, skipFirstRow),
      HeaderAsStringCSVParser(edgeHeader, delimiter),
      ConcatenateCSVsDataParser(edgeCSVs, delimiter, skipFirstRow),
      NumberedIdFromVertexField(vertexIdFieldName, sourceEdgeFieldName, destEdgeFieldName))

case class EdgeCSVImport(edgeHeader: Filename,
                         edgeCSVs: Seq[Filename],
                         vertexIdFieldName: String,
                         sourceEdgeFieldName: String,
                         destEdgeFieldName: String,
                         delimiter: String,
                         skipFirstRow: Boolean)
    extends ImportGraph(DummyMetaParser(),
      DummyDataParser(),
      HeaderAsStringCSVParser(edgeHeader, delimiter),
      ConcatenateCSVsDataParser(edgeCSVs, delimiter, skipFirstRow),
      IdFromEdgeFields(vertexIdFieldName, sourceEdgeFieldName, destEdgeFieldName))
