// Given two integer index attributes, this operation counts the number of occurrences of each
// possible pair of index values.
//
// This is used (at least) to create edge diagrams: for each edge, the first index is the node
// index in the source vertex diagram and the second index is the node index in the destination
// vertex diagram. So in this case this operation in effect computes how many edges go between two
// nodes in two vertex diagrams.

package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.RDDUtils

object IndexPairCounter extends OpFromJson {
  class Input extends MagicInputSignature {
    val original = vertexSet
    val filtered = vertexSet
    val xIndices = vertexAttribute[Int](filtered)
    val yIndices = vertexAttribute[Int](filtered)
    val weights = vertexAttribute[Double](original)
    val originalCount = scalar[Long]
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val counts = scalar[Map[(Int, Int), Double]]
  }
  def fromJson(j: JsValue) = IndexPairCounter()
}
import IndexPairCounter._
case class IndexPairCounter() extends SparkOperation[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val xIndices = inputs.xIndices.rdd
    val yIndices = inputs.yIndices.rdd

    output(
      o.counts,
      RDDUtils.estimateValueWeights(
        inputs.original.rdd,
        inputs.weights.rdd,
        xIndices.sortedJoin(yIndices).copartition(inputs.original.rdd),
        inputs.originalCount.value,
        50000,
        rc),
    )
  }
}
