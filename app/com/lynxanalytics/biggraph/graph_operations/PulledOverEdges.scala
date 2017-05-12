// This operation is useful when one wants to apply on an edge bundle a tranformative operation
//  (e.g. filter) that was defined on vertex sets.
//
// The way to go is to first apply the operation on edgeBundle.idSet, say it creates a new
// vertex set destinationVS and also provides an injection from destinationVS to
// edgeBundle.idSet. Then apply PullOverEdges with the original edge bundle and the above
// injection.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PulledOverEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val originalSrc = vertexSet
    val originalDst = vertexSet
    val originalIds = vertexSet
    val originalEB = edgeBundle(originalSrc, originalDst, idSet = originalIds)
    val destinationVS = vertexSet
    val injection = edgeBundle(destinationVS, originalIds, EdgeBundleProperties.injection)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val pulledEB = edgeBundle(
      inputs.originalSrc.entity,
      inputs.originalDst.entity,
      idSet = inputs.destinationVS.entity)
  }
  def fromJson(j: JsValue) = PulledOverEdges()
}
import PulledOverEdges._
case class PulledOverEdges()
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val injectionEntity = inputs.injection.meta
    val injection = inputs.injection.rdd
    val originalEB = inputs.originalEB.rdd
    val destinationVS = inputs.destinationVS.rdd
    val destinationPartitioner = destinationVS.partitioner.get
    val pulledEB =
      if (injectionEntity.properties.isIdPreserving) {
        val joinableOriginalEB = originalEB.sortUnique(destinationPartitioner)
        joinableOriginalEB.sortedJoin(destinationVS).mapValues { case (edge, _) => edge }
      } else {
        val originalToDestinationId = injection
          .map { case (id, edge) => (edge.dst, edge.src) }
          .sortUnique(originalEB.partitioner.get)
        originalEB.sortedJoin(originalToDestinationId)
          .map { case (originalId, (edge, destinationId)) => (destinationId, edge) }
          .sortUnique(destinationPartitioner)
      }
    output(o.pulledEB, pulledEB)
  }
}
