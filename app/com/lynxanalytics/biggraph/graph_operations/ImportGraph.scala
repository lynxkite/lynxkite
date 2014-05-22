package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD

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

  object SignatureReader {
    var signature = AttributeSignature.empty
    lazy val attributesData: DenseAttributes = signature.maker.make

    def readDouble(idx: AttributeWriteIndex[Double], value: String): Unit = {
      val convertedValue = if (value == "") 0 else value.toDouble
      attributesData.set(idx, convertedValue)
    }

    def readLong(idx: AttributeWriteIndex[Long], value: String): Unit = {
      val convertedValue = if (value == "") 0 else value.toLong
      attributesData.set(idx, convertedValue)
    }

    def readBoolean(idx: AttributeWriteIndex[Boolean], value: String): Unit = {
      val convertedValue = if (value.toLowerCase == "true") true else false
      attributesData.set(idx, convertedValue)
    }

    def readString(idx: AttributeWriteIndex[String], value: String): Unit = {
      val convertedValue = value.stripPrefix("\"").stripSuffix("\"")
      attributesData.set(idx, convertedValue)
    }
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