// Calculates dispersion, the extent to which two people's mutual friends are not themselves well-connected.
// Source: http://arxiv.org/abs/1310.6753
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable
import com.lynxanalytics.biggraph.graph_api._

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
  private def sortedHasCommon(a: Seq[ID], b: Seq[ID]): Boolean = {
    val ait = a.iterator.buffered
    val bit = b.iterator.buffered
    while (ait.hasNext && bit.hasNext) {
      if (ait.head == bit.head) {
        return true
      }
      if (ait.head > bit.head) {
        bit.next
      } else {
        ait.next
      }
    }
    return false
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
    val fullGraph = CompactUndirectedGraph(rc, inputs.es.data, needsBothDirections = false)
    val dispersion = nonLoopEdges.mapValues {
      case (edge) =>
        val srcNeighbors = fullGraph.getNeighbors(edge.src).filter(_ != edge.dst)
        val dstNeighbors = fullGraph.getNeighbors(edge.dst)
        val commonNeighbors = sortedIntersection(srcNeighbors, dstNeighbors)
        val commonNeighborsNeighbors = commonNeighbors
          .map(n => n -> sortedIntersection(fullGraph.getNeighbors(n), srcNeighbors))
          .toMap
        commonNeighbors.combinations(2).map {
          case Seq(a, b) =>
            val aNeighbors = commonNeighborsNeighbors(a)
            val bNeighbors = commonNeighborsNeighbors(b)
            if (aNeighbors.contains(b) || sortedHasCommon(aNeighbors, bNeighbors)) {
              0.0
            } else {
              1.0
            }
          case _ => 0.0
        }.sum
    }
    output(o.dispersion, dispersion)
  }
}
