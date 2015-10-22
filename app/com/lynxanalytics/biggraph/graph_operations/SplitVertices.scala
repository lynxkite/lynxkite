// Split vertices (sort of opposite of merge vertices

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SplitVertices extends OpFromJson {
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[_]) extends MagicOutput(instance) {

    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity,
      segments,
      EdgeBundleProperties(isReversedFunction = true, isReverseEverywhereDefined = true))
    val indexAttr = vertexAttribute[Long](segments)
  }
  def fromJson(j: JsValue) = SplitVertices()
}
import SplitVertices._
case class SplitVertices() extends TypedMetaGraphOp[VertexAttributeInput[Double], Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[Double]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val repetitionAttr = inputs.attr.rdd
      .map { case (oldId, rep) => (oldId, rep.floor.toLong) }

    val requestedNumberOfVerticesWithIndex =
      repetitionAttr.flatMapValues { numRepetitions => (1.toLong to numRepetitions) }

    val partitioner =
      rc.partitionerForNRows(requestedNumberOfVerticesWithIndex.count)

    val newAndAndOldIdAndIndex =
      requestedNumberOfVerticesWithIndex.randomNumbered(partitioner)

    output(o.segments,
      newAndAndOldIdAndIndex
        .mapValues(_ => ()).toSortedRDD(partitioner))
    output(o.belongsTo,
      newAndAndOldIdAndIndex
        .map { case (newId, (oldId, idx)) => newId -> Edge(oldId, newId) }
        .toSortedRDD(partitioner))
    output(o.indexAttr,
      newAndAndOldIdAndIndex
        .map { case (newId, (oldId, idx)) => newId -> idx }
        .toSortedRDD(partitioner))
  }
}
