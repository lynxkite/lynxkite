// Creates new segmentation links using the original links plus every
// neighbor (on the parent graph) of the members of each segment.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object GrowSegmentation extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsG = vertexSet
    val vsS = vertexSet
    val esG = edgeBundle(vsG, vsG)
    val esGS = edgeBundle(vsG, vsS)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val esGS = edgeBundle(inputs.vsG.entity, inputs.vsS.entity, EdgeBundleProperties.default)
  }
  def fromJson(j: JsValue) = GrowSegmentation()
}
import GrowSegmentation._
case class GrowSegmentation() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = RDDUtils.maxPartitioner(
      inputs.esGS.rdd.partitioner.get, inputs.esG.rdd.partitioner.get)
    // Links from vertices to segments.
    val esGS = inputs.esGS.rdd.map { case (_, s) => s.src -> s.dst }.sort(partitioner)
    val esG = inputs.esG.rdd.map { case (_, e) => e.src -> e.dst }.sort(partitioner)
    val neighborToSeg = (esG.sortedJoinWithDuplicates(esGS).map {
      case (src, (dst, segment)) => dst -> segment
    } ++ esGS).distinct.randomNumbered(partitioner)
    output(o.esGS, neighborToSeg.mapValues { case (vid, sid) => Edge(vid, sid) })
  }
}
