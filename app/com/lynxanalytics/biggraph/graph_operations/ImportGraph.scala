package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD

case class ImportGraph(
  // temporary place for inputs, these were not the final input arguments
  inputSignatures: Iterator[(String, String)],
  inputData: RDD[Iterator[String]]
    ) extends GraphOperation {

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  /* regex parsing might be an external step rather
  def parseInputSignature(input: Iterator[String], regex: Regex): Iterator[SignatureReader] = {
    input.map {
      case regex(sigName, sigType) => new SignatureReader(sigName, sigType)
    }
  }*/

  class SignatureReader(sigName: String, sigType: String) {
    var reader: (DenseAttributes, String) => Unit = null

    // TODO: refine this type mapping.
    sigType match {
      case "DECIMAL" | "DOUBLE" | "FLOAT" | "NUMERIC" | "REAL" =>
        SignatureReader.signature.addAttribute[Double](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Double](sigName)
        reader = SignatureReader.readDouble(idx, _: DenseAttributes, _: String)
      case "BIGINT" | "INTEGER" | "SMALLINT" | "SMALLUINT" | "TINYINT" | "TINYUINT" | "UINTEGER" =>
        SignatureReader.signature.addAttribute[Long](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Long](sigName)
        reader = SignatureReader.readLong(idx, _: DenseAttributes, _: String)
      case "BOOLEAN" =>
        SignatureReader.signature.addAttribute[Boolean](sigName: String)
        val idx = SignatureReader.signature.writeIndex[Boolean](sigName)
        reader = SignatureReader.readBoolean(idx, _: DenseAttributes, _: String)
      case _ => // default to string
        SignatureReader.signature.addAttribute[String](sigName: String)
        val idx = SignatureReader.signature.writeIndex[String](sigName)
        reader = SignatureReader.readString(idx, _: DenseAttributes, _: String)
    }

    def read(value: String) = reader(SignatureReader.signature.maker.make, value) // WTF is this maker.make?
  }

  object SignatureReader {
    var signature = AttributeSignature.empty

    def readDouble(idx: AttributeWriteIndex[Double], attributesData: DenseAttributes, value: String): Unit = {
      val convertedValue = if (value == "") 0 else value.toDouble
      attributesData.set(idx, convertedValue)
    }

    def readLong(idx: AttributeWriteIndex[Long], attributesData: DenseAttributes, value: String): Unit = {
      val convertedValue = if (value == "") 0 else value.toLong
      attributesData.set(idx, convertedValue)
    }

    def readBoolean(idx: AttributeWriteIndex[Boolean], attributesData: DenseAttributes, value: String): Unit = {
      val convertedValue = if (value.toLowerCase == "true") true else false
      attributesData.set(idx, convertedValue)
    }

    def readString(idx: AttributeWriteIndex[String], attributesData: DenseAttributes, value: String): Unit = {
      val convertedValue = value.stripPrefix("\"").stripSuffix("\"")
      attributesData.set(idx, convertedValue)
    }
  }
  // they can be edge and vertex readers, so they produce either edge or vertex attributes
  // edge reader is a special reader that also creates an edge???

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext

    val signatureReaders = inputSignatures.map(x => new SignatureReader(x._1, x._2))

    // reads and stores the data, the unprocessed lines can be used for logging
    val excludedData = inputData.flatMap { line =>
      if (line.size == signatureReaders.size) {
        signatureReaders.zip(line).foreach {
          case (reader, data) => reader.read(data)
        }
        None
      } else {
        Some(line)
      }
    }

    /*
     * optional: conditions for handling bad ids, bad files
     * index vertices
     * optionally provide triplets (we don't have them implemented in GraphDataManager yet)
     */

    return ???
  }
  /*
  possible input formats
  - vertex list + edge list (1 file)
  - adjacency list
  - adjacency matrix
  - vertex list only
  - edge list only
  - as JSON from FE

  possible attribute formats
  - vertex/edge attributes in separate file(s)
  - vertex/edge attributes as header
  - as JSON from FE
  */

  // The vertex attribute signature of the graph resulting from this operation.
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = ???

  // The edge attribute signature of the graph resulting from this operation.
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = ???
}