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

trait MetaDataReader {
  val signature: AttributeSignature = AttributeSignature.empty
  def createSnatchers(): Seq[AttributeSnatcher[String]]
}
case class HeaderAsStringCSVReader(sc: spark.SparkContext, inputFile: Filename, delimiter: String = ",") extends MetaDataReader {
  val header = sc.textFile(inputFile.path.toString).first.split("""\""" + delimiter)
  def createSnatchers: Seq[AttributeSnatcher[String]] = {
    header.map { sigName =>
      signature.addAttribute[String](sigName: String)
      val idx = signature.writeIndex[String](sigName)
      StringSnatcher(idx)
    }
  }
}

trait RawDataProvider {
  def getRawData(sc: spark.SparkContext): RDD[Seq[String]]
}
case class ConcatenateCSVsDataProvider(files: Seq[Filename], delimiter: String = ",") {
  def getRawData(sc: spark.SparkContext, bytesPerPartition: Long = 10000000L): RDD[Seq[String]] = {
    // what happens to partitioning after union?
    val lines = sc.union(files.map { filename =>
      val partitions = (filename.fileSize / bytesPerPartition + 1).toInt
      sc.textFile(filename.path.toString, partitions)
    })
    lines.map(_.split("""\""" + delimiter))
  }
}

trait GraphBuilder {
  def buildGraph(vertexDataSig: AttributeSignature,
                 vertexData: RDD[DenseAttributes],
                 edgeDataSig: AttributeSignature,
                 edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD)
}
case class NumberedIdFromVertexField(vertexIdFieldName: String,
                                     sourceEdgeFieldName: String,
                                     destEdgeFieldName: String) {
  def buildGraph(vertexDataSig: AttributeSignature,
                 vertexData: RDD[DenseAttributes],
                 edgeDataSig: AttributeSignature,
                 edgeData: RDD[DenseAttributes]): (VertexRDD, EdgeRDD) = {
    val vertices: VertexRDD = spark_util.RDDUtils.fastNumbered(vertexData)
    val vIdx = vertexDataSig.readIndex[String](vertexIdFieldName)
    // propagate new ids to edges
    val vertexFieldToId = vertices.map { case (vertexId, vertexAttributes) =>
      vertexAttributes(vIdx) -> vertexId
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

class ReadCSV(vertexDataProvider: RawDataProvider,
              vertexMetaProvider: MetaDataReader,
              edgeDataProvider: RawDataProvider,
              edgeMetaProvider: MetaDataReader,
              graphLinker: GraphBuilder)
    extends GraphOperation {
  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    return ???
  }

  private def readToDenseAttributes[T](inputData: RDD[Seq[T]],
                                       inputSignatures: AttributeSignature,
                                       snatchers: Seq[AttributeSnatcher[T]]): RDD[DenseAttributes] = {
    // TODO: this is obviously not good like this, will continue from here...
    inputData.foreach { line =>
      if (line.size == snatchers.size) {
        snatchers.zip(line).foreach { valid =>
          val empty = AttributeSignature.empty.maker.make
          valid match { case (snatcher, data) => snatcher(data, empty) }
        }
      }
    }
  }
}

//case class GlobeImport(...) extends ReadCSV(TGZDataProvider(files, filepattern), )

//***********************************************


/*
object SignatureReader {
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




case class ImportGraph(
  vertexFiles: Seq[String] = Seq(),
  edgeFiles: Seq[String] = Seq(),
  vertexSigFile: String = "",
  edgeSigFile: String = "",
  // true: csv header parsing, false: regex parsing
  vertexIsCSV: Boolean = true,
  edgeIsCSV: Boolean = true,
  vertexParser: String = ",",
  edgeParser: String = ","
    ) extends GraphOperation {

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  def readGraph(
    inputSignatures: Iterator[(String, String)],
    inputData: RDD[Iterator[String]]) = {
    val signatureReaders = inputSignatures.map(x => new SignatureReader(x._1, x._2))

    val excludedData = inputData.flatMap { line =>
      if (line.size == signatureReaders.size) {
        signatureReaders.zip(line).foreach {
          case (reader, data) => reader.read(data) // stores valid data
        }
        None
      } else {
        Some(line) // we can log the excluded data
      }
    }
  }

  var signature = AttributeSignature.empty

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


  // TODO: should this go into the graph_util or spark_util package?
  import org.apache.hadoop
  import org.apache.spark

  def fileSize(filename: String, sc: spark.SparkContext = null): Long = {
    val hconf = if (sc != null) sc.hadoopConfiguration else new hadoop.conf.Configuration()
    val fs = hadoop.fs.FileSystem.get(new java.net.URI(filename), hconf)
    val status = fs.getFileStatus(new hadoop.fs.Path(filename))
    return status.getLen
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


  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    //getRawDataRDD to get edges and vertices as RDD[Iterator[String]]
    //readGraph to create reader objects, read signature and set DenseAttributes

    // We cannot have more vertex partitions than edge partitions. (SPARK-1329)
    // val xParts = vertexParts max edgeParts

    // handle various compressions
    // we don't need to include AWS data in order to cope with AWS, do we now?

    // reindex vertices in case needed
    // after attributes are all set, parse edge and vertex information and create final edge and vertex RDDs
    // create final graph objects

    /*
     * nice to have: handling bad ids, bad files etc. + nice logging
     * optionally provide triplets (we don't have them implemented in GraphDataManager yet)
     * optional: handle adjacency list instead edge list
     * FE idea: load sample of a file, visualize as table, user can set the type of the fields and send that as a JSON in case types are missing
     * FE idea: test regex on a small sample
     */

    return ???
  }

  // The vertex attribute signature of the graph resulting from this operation.
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = ???

  // The edge attribute signature of the graph resulting from this operation.
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = ???
}
*/
