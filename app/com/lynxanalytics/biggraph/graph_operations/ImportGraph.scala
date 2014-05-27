package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api.attributes._
import com.lynxanalytics.biggraph.spark_util
//import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark
import org.apache.spark.graphx
import java.util.regex.Pattern

/*
 * For a sequence of raw input meta data strings a sequence of AttributeSnatchers
 * will be instantiated. These will format their given input string to the provided
 * type and write the data into a DenseAttribute object at a given index as a side effect.
 * The sequence usually corresponds to a line of input file, the data will result
 * one or two DenseAttribute objects (for vertex and/or edge attributes).
 */
trait AttributeSnatcher[T] {
  def apply(input: T, attributes: DenseAttributes): Unit
}
case class StringSnatcher(idx: AttributeWriteIndex[String]) extends AttributeSnatcher[String] {
  def apply(input: String, attributes: DenseAttributes): Unit = {
    val strippedInput = input.stripPrefix("\"").stripSuffix("\"")
    attributes.set(idx, strippedInput)
  }
}

trait MetaDataParser {
  def getSignature(): AttributeSignature
  def createSnatchers(signature: AttributeSignature): Seq[AttributeSnatcher[String]]
}
case class HeaderAsStringCSVParser(inputFile: Filename, delimiter: String) extends MetaDataParser {
  val csvHeader = inputFile.open.readLine // deprecated
    .split(Pattern.quote(delimiter))
    .map(sigName => sigName.stripPrefix("\"").stripSuffix("\""))

  def getSignature(): AttributeSignature = {
    var signature: AttributeSignature = AttributeSignature.empty
    csvHeader.foreach { sigName =>
      signature = signature.addAttribute[String](sigName).signature
    }
    signature
  }

  def createSnatchers(signature: AttributeSignature): Seq[AttributeSnatcher[String]] = {
    csvHeader.map { sigName =>
      val idx = signature.writeIndex[String](sigName)
      StringSnatcher(idx)
    }
  }
}
case class DummyMetaParser() extends MetaDataParser {
  def getSignature(): AttributeSignature = AttributeSignature.empty
  def createSnatchers(signature: AttributeSignature): Seq[AttributeSnatcher[String]] = Seq()
}

trait RawDataParser {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]]
}
case class ConcatenateCSVsDataParser(inputFiles: Seq[Filename],
                                     delimiter: String,
                                     skipFirstRow: Boolean) extends RawDataParser {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]] = {
    val header = if (skipFirstRow) sc.textFile(inputFiles.head.filename).first else null
    val lines = sc.union(inputFiles.map(file => sc.textFile(file.filename).filter(_ != header)))
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
case class NumberedIdFromVertexField(vertexIdFieldName: String,
                                     sourceEdgeFieldName: String,
                                     destEdgeFieldName: String) extends GraphBuilder {
  def build(vertexDataSig: AttributeSignature,
            vertexData: RDD[DenseAttributes],
            edgeDataSig: AttributeSignature,
            edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD) = {
    val vertices: VertexRDD = spark_util.RDDUtils.fastNumbered(vertexData)
    val vIdx = vertexDataSig.readIndex[String](vertexIdFieldName)
    // propagate new ids to edges
    val vertexFieldToId = vertices.map {
      case (vertexId, vertexAttributes) => vertexAttributes(vIdx) -> vertexId
    } // will need to be cached?
    val eSrcIdx = edgeDataSig.readIndex[String](sourceEdgeFieldName)
    val eDstIdx = edgeDataSig.readIndex[String](destEdgeFieldName)
    val edgesBySource = edgeData.map( edgeAttributes =>
      edgeAttributes(eSrcIdx) -> (edgeAttributes(eDstIdx), edgeAttributes)
    )
    val edgesByDest = edgesBySource.join(vertexFieldToId).map {
      case (src, ((dst, eAttr), srcId)) => dst -> (srcId, eAttr)
    }
    val edges: EdgeRDD = edgesByDest.join(vertexFieldToId).map {
      case (dst, ((srcId, eAttr), dstId)) => graphx.Edge(srcId, dstId, eAttr)
    }
    // TODO: handle partitioning
    // TODO: what if field is not String?
    return (vertices, edges)
  }
}
case class IdFromEdgeFields(vertexIdFieldName: String,
                            sourceEdgeFieldName: String,
                            destEdgeFieldName: String) extends GraphBuilder {
  def build(vertexDataSig: AttributeSignature,
            vertexData: RDD[DenseAttributes],
            edgeDataSig: AttributeSignature,
            edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD) = {
    val newVertexSig = vertexDataSig.addAttribute[String](vertexIdFieldName).signature
    val maker = newVertexSig.maker
    val vWriteIdx = newVertexSig.writeIndex[String](vertexIdFieldName)
    val vReadIdx = newVertexSig.readIndex[String](vertexIdFieldName)
    val eSrcIdx = edgeDataSig.readIndex[String](sourceEdgeFieldName)
    val eDstIdx = edgeDataSig.readIndex[String](destEdgeFieldName)
    // could be made faster probably by avoiding a DA read?
    val verticesData = edgeData.flatMap(edgeAttributes =>
      Seq(edgeAttributes(eSrcIdx), edgeAttributes(eDstIdx))).distinct
    val verticesFromEdges = verticesData.map(maker.make.set(vWriteIdx, _))
    val vertices: VertexRDD = spark_util.RDDUtils.fastNumbered(verticesFromEdges)
    // propagate new ids to edges
    val vertexFieldToId = vertices.map {
      case (vertexId, vertexAttributes) => vertexAttributes(vReadIdx) -> vertexId
    } // will need to be cached?
    val edgesBySource = edgeData.map( edgeAttributes =>
      edgeAttributes(eSrcIdx) -> (edgeAttributes(eDstIdx), edgeAttributes)
    )
    val edgesByDest = edgesBySource.join(vertexFieldToId).map {
      case (src, ((dst, eAttr), srcId)) => dst -> (srcId, eAttr)
    }
    val edges: EdgeRDD = edgesByDest.join(vertexFieldToId).map {
      case (dst, ((srcId, eAttr), dstId)) => graphx.Edge(srcId, dstId, eAttr)
    }
    // TODO: handle partitioning
    // TODO: what if field is not String?
    return (vertices, edges)
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

    val vertexSignature = vertexMeta.getSignature
    val edgeSignature = edgeMeta.getSignature
    val vertexSnatchers = vertexMeta.createSnatchers(vertexSignature)
    val edgeSnatchers = edgeMeta.createSnatchers(edgeSignature)
    val rawVertices = vertexData.getRawData(sc)
    val rawEdges = edgeData.getRawData(sc)
    val vertexDenseAttributes = rawToDenseAttributes(vertexSignature.maker, vertexSnatchers, rawVertices)
    val edgeDenseAttributes = rawToDenseAttributes(edgeSignature.maker, edgeSnatchers, rawEdges)
    val (vertices, edges) = graphBuilder.build(vertexSignature, vertexDenseAttributes, edgeSignature, edgeDenseAttributes)
    new SimpleGraphData(target, vertices, edges) // later optional triplets?
  }

  // The vertex attribute signature of the graph resulting from this operation.
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = vertexSignature

  // The edge attribute signature of the graph resulting from this operation.
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = edgeSignature

  private def rawToDenseAttributes[T](attributesMaker: DenseAttributesMaker,
                                      snatchers: Seq[AttributeSnatcher[T]],
                                      inputData: RDD[Seq[T]]): RDD[DenseAttributes] = {
    inputData.flatMap { line =>
      if (line.size == snatchers.size) {
        val attributes = attributesMaker.make
        snatchers.zip(line).foreach {
          case (snatcher, data) => snatcher(data, attributes)
        }
        Some(attributes)
      } else {
        None // might be nice to log this
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
                        NumberedIdFromVertexField(vertexIdFieldName,
                                                  sourceEdgeFieldName,
                                                  destEdgeFieldName))

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
                        IdFromEdgeFields(vertexIdFieldName,
                                         sourceEdgeFieldName,
                                         destEdgeFieldName))


//**** DEV NOTES:

//case class GlobeImport(...) extends ReadCSV(TGZDataProvider(files, filepattern), )


/*
object Snatchers {
  def snatchDouble(idx: AttributeWriteIndex[Double], value: String): Unit = {
    val convertedValue = if (value == "") 0 else value.toDouble
    attributesData.set(idx, convertedValue)
  }

  def snatchLong(idx: AttributeWriteIndex[Long], value: String): Unit = {
    val convertedValue = if (value == "") 0 else value.toLong
    attributesData.set(idx, convertedValue)
  }

  def snatchBoolean(idx: AttributeWriteIndex[Boolean], value: String): Unit = {
    val convertedValue = if (value.toLowerCase == "true") true else false
    attributesData.set(idx, convertedValue)
  }

  def snatchString(idx: AttributeWriteIndex[String], value: String): Unit = {
    val convertedValue = value.stripPrefix("\"").stripSuffix("\"")
    attributesData.set(idx, convertedValue)
  }
}



  class SignatureReader(sigName: String, sigType: String) {
    var reader: String => Unit = null

    sigType match {
      case "DECIMAL" | "DOUBLE" | "FLOAT" | "NUMERIC" | "REAL" =>
        SignatureReader.signature.addAttribute[Double](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Double](sigName)
        reader = SignatureReader.readDouble(idx, _: String)
      case "BIGINT" | "INTEGER" | "SMALLINT" | "SMALLUINT" | "TINYINT" | "TINYUINT" | "UINTEGER" =>
        SignatureReader.signature.addAttribute[Long](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Long](sigName)
        reader = SignatureReader.readLong(idx, _: String)
      case "BOOLEAN" =>
        SignatureReader.signature.addAttribute[Boolean](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Boolean](sigName)
        reader = SignatureReader.readBoolean(idx, _: String)
      case _ => // defaults to string
        SignatureReader.signature.addAttribute[String](sigName: String)
        val idx = SignatureReader.signature.writeIndex[String](sigName)
        reader = SignatureReader.readString(idx, _: String)
    }

    def read(value: String) = reader(value)
  }


  def getRawDataRDD(
      sc: spark.SparkContext,
      files: Seq[String],
      bytesPerShard: Long = 1000000000L): RDD[Iterator[String]] = {
    val shards = files.map(filename => (filename, (fileSize(filename, sc) / bytesPerShard + 1).toInt))
    val sharded = shards.flatMap {
      case (filename, shardsize) => (0 until shardsize).map((filename, _, shardsize))
    }
    val shuffled = scala.util.Random.shuffle(sharded.toSeq).toArray
    val filesAsRDD = sc.parallelize(shuffled, shuffled.length)
    // TODO: parse this file into lines of data: csv+separator or regex
    return ???
  }

  // TODO: clean this up
  def parseInputSignature(input: Iterator[String], regex: Regex): Iterator[SignatureReader] = {
    input.map {
      case regex(sigName, sigType) => new SignatureReader(sigName, sigType)
    }
  }


  /*
   * nice to have: handling bad ids, bad files etc. + nice logging
   * optionally provide triplets (we don't have them implemented in GraphDataManager yet)
   * optional: handle adjacency list instead edge list
   * FE idea: load sample of a file, visualize as table, user can set the type of the fields and send that as a JSON in case types are missing
   * FE idea: test regex on a small sample
   */
*/
