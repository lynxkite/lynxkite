// Maps an attribute from one vertex set to another.
//
// The purpose of this is to update the attributes after an operation has
// modified the vertex set. For example after filtering the vertices the
// attribute values that belonged to discarded vertices need to be discarded
// as well. You create a PulledOverVertexAttribute that follows the mapping
// from the unfiltered vertex set to the filtered one.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PulledOverVertexAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val originalVS = vertexSet
    val destinationVS = vertexSet
    val function = edgeBundle(destinationVS, originalVS, EdgeBundleProperties.partialFunction)
    val originalAttr = vertexAttribute[T](originalVS)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.originalAttr.typeTag
    val pulledAttr = vertexAttribute[T](inputs.destinationVS.entity)
  }
  def pullAttributeVia[T](
    originalAttr: Attribute[T], function: EdgeBundle)(
      implicit metaManager: MetaGraphManager): Attribute[T] = {
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val pop = PulledOverVertexAttribute[T]()
    pop(pop.originalAttr, originalAttr)(pop.function, function).result.pulledAttr
  }
  def fromJson(j: JsValue) = PulledOverVertexAttribute()
}
case class PulledOverVertexAttribute[T]()
    extends TypedMetaGraphOp[PulledOverVertexAttribute.Input[T], PulledOverVertexAttribute.Output[T]] {
  import PulledOverVertexAttribute._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val functionEntity = inputs.function.meta
    val function = inputs.function.rdd
    val originalAttr = inputs.originalAttr.rdd
    val originalPartitioner = originalAttr.partitioner.get
    implicit val ct = inputs.originalAttr.meta.classTag
    val destinationVS = inputs.destinationVS.rdd
    val destinationPartitioner = destinationVS.partitioner.get
    val pulledAttr =
      if (functionEntity.properties.isIdPreserving) {
        val joinableFunction = function.sortUnique(originalPartitioner)
        val originallyPartitioned =
          originalAttr.sortedJoin(joinableFunction).mapValues { case (value, edge) => value }
        originallyPartitioned.sortedRepartition(destinationPartitioner)
      } else {
        val originalToDestinationId = function
          .map { case (id, edge) => (edge.dst, edge.src) }
          .sort(originalPartitioner)
        originalToDestinationId.sortedJoin(originalAttr)
          .values
          .sortUnique(destinationPartitioner)
      }
    output(o.pulledAttr, pulledAttr)
  }
}
