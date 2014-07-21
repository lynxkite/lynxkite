package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._

object VertexBucketGrid {
  class Input[S, T] extends MagicInputSignature {
    val vertices = vertexSet
    val xAttribute = vertexAttribute[S](vertices)
    val yAttribute = vertexAttribute[T](vertices)
  }
  class Output[S, T](implicit instance: MetaGraphOperationInstance,
                     inputs: Input[S, T]) extends MagicOutput(instance) {
    implicit val xtt = inputs.xAttribute.typeTag
    implicit val ytt = inputs.yAttribute.typeTag
    val bucketSizes = scalar[Map[(Int, Int), Int]]
    val xBuckets = vertexAttribute[Int](inputs.vertices.entity)
    val yBuckets = vertexAttribute[Int](inputs.vertices.entity)
    val feIdxs = vertexAttribute[Int](inputs.vertices.entity)
  }
}
import VertexBucketGrid._
case class VertexBucketGrid[S, T](xBucketer: Bucketer[S],
                                  yBucketer: Bucketer[T])
    extends TypedMetaGraphOp[Input[S, T], Output[S, T]] {

  @transient override lazy val inputs = new Input[S, T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[S, T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val xtt = inputs.xAttribute.data.typeTag
    implicit val xct = inputs.xAttribute.data.classTag
    implicit val ytt = inputs.yAttribute.data.typeTag
    implicit val yct = inputs.yAttribute.data.classTag
    val vertices = inputs.vertices.rdd
    val xBuckets = if (xBucketer.numBuckets == 1) {
      vertices.mapValues(_ => 0)
    } else {
      implicit val tt = xBucketer.tt
      val xAttr = inputs.xAttribute.rdd
      vertices.join(xAttr).mapValues { case (_, value) => xBucketer.whichBucket(value) }
    }
    val yBuckets = if (yBucketer.numBuckets == 1) {
      vertices.mapValues(_ => 0)
    } else {
      implicit val tt = yBucketer.tt
      val yAttr = inputs.yAttribute.rdd
      vertices.join(yAttr).mapValues { case (_, value) => yBucketer.whichBucket(value) }
    }
    output(o.xBuckets, xBuckets)
    output(o.yBuckets, yBuckets)
    output(o.feIdxs,
      xBuckets.join(yBuckets).mapValues { case (x, y) => x * yBucketer.numBuckets + y })
    output(o.bucketSizes,
      xBuckets.join(yBuckets)
        .map { case (vid, (xB, yB)) => ((xB, yB), 1) }
        .reduceByKey(_ + _)
        .collect
        .toMap)
  }
}
