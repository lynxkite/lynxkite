// Calculates dispersion, the extent to which two people's mutual friends are not themselves well-connected.
// Source: http://arxiv.org/abs/1310.6753
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Dispersion extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val dispersion = edgeAttribute[Double](inputs.es.entity)
  }

  private[graph_operations] def sortedIntersection(a: Seq[ID], b: Seq[ID]): Seq[ID] = {
    val builder = new mutable.ArrayBuilder.ofLong
    var ai = 0
    var bi = 0
    while (ai < a.size && bi < b.size) {
      if (a(ai) == b(bi)) {
        builder += a(ai)
        ai += 1
        bi += 1
      } else if (a(ai) > b(bi)) {
        bi += 1
      } else {
        ai += 1
      }
    }
    builder.result()
  }
  def fromJson(j: JsValue) = Dispersion()
}
import Dispersion._
case class Dispersion() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val nonLoopEdges = edges.filter { case (_, e) => e.src != e.dst }
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val fullGraph = CompactUndirectedGraph(rc, inputs.es.data, needsBothDirections = false)
    val dispersion = nonLoopEdges.mapValues {
      case (edge) =>
        val srcNeighbors = fullGraph.getNeighbors(edge.src)
        val dstNeighbors = fullGraph.getNeighbors(edge.dst)
        val commonNeighbors = sortedIntersection(srcNeighbors, dstNeighbors)
        commonNeighbors.combinations(2).map {
          case Seq(a, b) =>
            val aNeighbors = fullGraph.getNeighbors(a)
            val bNeighbors = fullGraph.getNeighbors(b)
            if (aNeighbors.contains(b) || bNeighbors.contains(a)) {
              0.0
            } else {
              val neighborIntersection =
                sortedIntersection(
                  sortedIntersection(aNeighbors, bNeighbors),
                  srcNeighbors)
              if (neighborIntersection.count(_ != edge.dst) == 0) 1.0 else 0.0
            }
          case _ => 0.0
        }.sum
    }
    output(o.dispersion, dispersion)
  }
}
