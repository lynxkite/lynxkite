package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EdgeIndexer {
  class Input[T] extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val eb = edgeBundle(srcVS, dstVS)
    val filtered = edgeBundle(srcVS, dstVS)
    val baseIndices = edgeAttribute[Int](filtered)
    val bucketAttribute = edgeAttribute[T](eb)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val indices = edgeAttribute[Int](inputs.filtered.entity)
  }
}
import EdgeIndexer._
case class EdgeIndexer[T](bucketer: Bucketer[T])
    extends TypedMetaGraphOp[Input[T], Output[T]] {

  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val filtered = inputs.filtered.rdd
    val bucketAttribute = inputs.bucketAttribute.rdd
    val buckets =
      filtered.sortedJoin(bucketAttribute).mapValues {
        case (_, value) => bucketer.whichBucket(value)
      }.asSortedRDD
    val baseIndices = inputs.baseIndices.rdd
    output(
      o.indices,
      baseIndices.sortedJoin(buckets)
        .mapValues { case (baseIndex, bucket) => bucketer.numBuckets * baseIndex + bucket }
        .asSortedRDD)
  }
}
