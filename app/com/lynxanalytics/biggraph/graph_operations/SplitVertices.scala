// Split vertices (sort of opposite of merge vertices)

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SplitVertices extends OpFromJson {
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[Long]) extends MagicOutput(instance) {

    val newVertices = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity,
      newVertices,
      EdgeBundleProperties(isReversedFunction = true, isReverseEverywhereDefined = true))
    val indexAttr = vertexAttribute[Long](newVertices)
  }
  def fromJson(j: JsValue) = SplitVertices()
}
import SplitVertices._
case class SplitVertices() extends TypedMetaGraphOp[VertexAttributeInput[Long], Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[Long]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val repetitionAttr = inputs.attr.rdd

    val requestedNumberOfVerticesWithIndex =
      repetitionAttr.flatMapValues { numRepetitions => (1.toLong to numRepetitions) }

    val partitioner =
      rc.partitionerForNRows(requestedNumberOfVerticesWithIndex.count)

    val newIdAndOldIdAndZeroBasedIndex =
      requestedNumberOfVerticesWithIndex
        .map { case (oldId, index) => (oldId, index - 1) }
        .randomNumbered(partitioner)

    output(o.newVertices,
      newIdAndOldIdAndZeroBasedIndex
        .mapValues(_ => ()).toSortedRDD(partitioner))
    output(o.belongsTo,
      newIdAndOldIdAndZeroBasedIndex
        .map { case (newId, (oldId, idx)) => newId -> Edge(oldId, newId) }
        .toSortedRDD(partitioner))
    output(o.indexAttr,
      newIdAndOldIdAndZeroBasedIndex
        .map { case (newId, (oldId, idx)) => newId -> idx }
        .toSortedRDD(partitioner))
  }
}
