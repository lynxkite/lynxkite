package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object EdgeIndexPairCounter {
  class Input extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val original = edgeBundle(srcVS, dstVS)
    val filtered = edgeBundle(srcVS, dstVS)
    val xIndices = edgeAttribute[Int](filtered)
    val yIndices = edgeAttribute[Int](filtered)
    val originalCount = scalar[Long]
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val counts = scalar[Map[(Int, Int), Int]]
  }
}
import EdgeIndexPairCounter._
case class EdgeIndexPairCounter() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val xIndices = inputs.xIndices.rdd
    val yIndices = inputs.yIndices.rdd

    output(
      o.counts,
      RDDUtils.estimateValueCounts(
        inputs.original.rdd,
        xIndices.sortedJoin(yIndices),
        inputs.originalCount.value,
        50000))
  }
}
