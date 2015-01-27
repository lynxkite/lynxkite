package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

/* This operation is used to compute a single index for each vertex based on potentially multiple
 * bucketings. This is basically done as a "product" of per attribute buckets. For a detailed
 * specification, see the comment for VertexView's indexingSeq field.
 *
 * One application of this operation computes one step in the product, that is given the index
 * based on the previous bucketed attributes (baseIndices), it computes the index based on the
 * previous and the current bucketed attribute.
 */
object Indexer extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vs = vertexSet
    val filtered = vertexSet
    val baseIndices = vertexAttribute[Int](filtered)
    val bucketAttribute = vertexAttribute[T](vs)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val indices = vertexAttribute[Int](inputs.filtered.entity)
  }
  def fromJson(j: JsValue) = Indexer(null)
}
import Indexer._
case class Indexer[T](bucketer: Bucketer[T])
    extends TypedMetaGraphOp[Input[T], Output[T]] {
  assert(bucketer != null)

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
      filtered.sortedJoin(bucketAttribute).flatMapValues {
        case (_, value) => bucketer.whichBucket(value)
      }
    val baseIndices = inputs.baseIndices.rdd
    output(
      o.indices,
      baseIndices.sortedJoin(buckets)
        .mapValues { case (baseIndex, bucket) => bucketer.numBuckets * baseIndex + bucket })
  }
}
