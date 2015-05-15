// Calculates the modularity score of a graph segmentation.
//
// The modularity of a segment is the fraction of edges inside it minus the
// fraction of edges expected to fall inside of it based on the in and out
// degrees. The modularity of the segmentation is the sum of the segment
// modularity scores.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Modularity extends OpFromJson {
  class Input() extends MagicInputSignature {
    val (vs, edges) = graph
    val segments = vertexSet
    val weights = edgeAttribute[Double](edges)
    val belongsTo = edgeBundle(
      vs, segments, requiredProperties = EdgeBundleProperties.partialFunction)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val modularity = scalar[Double]
  }
  def fromJson(j: JsValue) = Modularity()
}
import Modularity._
case class Modularity() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vPart = inputs.vs.rdd.partitioner.get
    val vToS = inputs.belongsTo.rdd.map { case (eid, e) => (e.src, e.dst) }.toSortedRDD(vPart)
    val bySrc = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map { case (eid, (e, w)) => (e.src, (e.dst, w)) }
      .toSortedRDD(vPart)
    val byDst = bySrc.sortedJoin(vToS)
      .map { case (src, ((dst, w), srcS)) => (dst, (srcS, w)) }
      .toSortedRDD(vPart)
    val segmentEdges = byDst.sortedJoin(vToS)
      .map { case (dst, ((srcS, w), dstS)) => ((srcS, dstS), w) }
      .reduceBySortedKey(vPart, _ + _)
    segmentEdges.cache()
    val outDegSum = segmentEdges
      .map { case ((srcS, dstS), w) => (srcS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val inDegSum = segmentEdges
      .map { case ((srcS, dstS), w) => (dstS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val fullEdges = segmentEdges
      .filter { case ((srcS, dstS), w) => (srcS == dstS) }
      .map { case ((srcS, dstS), w) => (srcS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val numEdges = outDegSum.values.reduce(_ + _)

    output(
      o.modularity,
      outDegSum.sortedJoin(inDegSum).fullOuterJoin(fullEdges)
        .map {
          case (s, (outsInsOpt, fullsOpt)) =>
            val (outs, ins) = outsInsOpt.getOrElse((0.0, 0.0))
            val fulls = fullsOpt.getOrElse(0.0)
            fulls / numEdges - (ins / numEdges) * (outs / numEdges)
        }
        .reduce(_ + _))
  }
}
