package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes._

// For a graph that has a vertex attribute of Array[VertexId], creates a graph
// representing the reverse mapping. IDs in the new graph will come from the
// `inputAttribute`, while `outputAttribute` will come from the the old IDs.
// No edges are generated. Non-long vertex attributes are discarded.
case class ExpandVertexSet[T](
    inputAttribute: String,
    outputAttribute: String) extends GraphOperation {

  type VSet = Array[VertexId]

  def isSourceListValid(sources: Seq[BigGraph]): Boolean = (
    sources.size == 1
    && sources.head.vertexAttributes.canRead[VSet](inputAttribute)
    && !sources.head.vertexAttributes.canRead[Long](outputAttribute)
  )

  def execute(target: BigGraph,
              manager: GraphDataManager): GraphData = {
    val inputGraph = target.sources.head
    val inputData = manager.obtainData(inputGraph)
    val runtimeContext = manager.runtimeContext
    val sc = runtimeContext.sparkContext
    val cores = runtimeContext.numAvailableCores
    val inputSig = inputGraph.vertexAttributes
    val inputIdx = inputSig.readIndex[VSet](inputAttribute)
    val outputSig = vertexAttributes(target.sources)
    val outputIdx = outputSig.writeIndex[VSet](outputAttribute)
    val maker = outputSig.maker
    val longs = inputSig.getAttributesReadableAs[Long]
    val indices: Seq[(AttributeReadIndex[Long], AttributeWriteIndex[VSet])] =
      longs.map {
        n => inputSig.readIndex[Long](n) -> outputSig.writeIndex[VSet](n)
      }

    val expanded = inputData.vertices.flatMap { case (id, attr) => attr(inputIdx).map(_ -> (id, attr)) }
    val grouped = expanded.groupByKey(runtimeContext.defaultPartitioner)
    val vertices = grouped.mapValues {
      group =>
        val (ids, attrs) = group.unzip
        val da = maker.make
        indices.foreach {
          case (src, dst) => da.set(dst, attrs.map(_(src)).toSet.toArray.sorted)
        }
        da.set(outputIdx, ids.toArray.sorted)
    }
    return new SimpleGraphData(
      target,
      vertices,
      sc.emptyRDD[spark.graphx.Edge[DenseAttributes]])
  }

  def vertexAttributes(inputGraphSpecs: Seq[BigGraph]) = {
    val longs = inputGraphSpecs.head.vertexAttributes.getAttributesReadableAs[Long]
    longs.foldLeft(AttributeSignature.empty) {
      case (sig, name) => sig.addAttribute[VSet](name).signature
    }.addAttribute[VSet](outputAttribute).signature
  }

  def edgeAttributes(inputGraphSpecs: Seq[BigGraph]) = AttributeSignature.empty

  override def targetProperties(inputGraphSpecs: Seq[BigGraph]) =
    new BigGraphProperties(symmetricEdges = true) // No edges.
}
