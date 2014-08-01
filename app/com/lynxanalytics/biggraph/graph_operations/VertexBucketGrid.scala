package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object VertexBucketGrid {
  class Input[S, T](xBucketed: Boolean, yBucketed: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val xAttribute = if (xBucketed) vertexAttribute[S](vertices) else null
    val yAttribute = if (yBucketed) vertexAttribute[T](vertices) else null
    val filtered = vertexSet
  }
  class Output[S, T](implicit instance: MetaGraphOperationInstance,
                     inputs: Input[S, T]) extends MagicOutput(instance) {
    val bucketSizes = scalar[Map[(Int, Int), Int]]
    val xBuckets = vertexAttribute[Int](inputs.filtered.entity)
    val yBuckets = vertexAttribute[Int](inputs.filtered.entity)
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
}
import VertexBucketGrid._
case class VertexBucketGrid[S, T](xBucketer: Bucketer[S],
                                  yBucketer: Bucketer[T])
    extends TypedMetaGraphOp[Input[S, T], Output[S, T]] {

  @transient override lazy val inputs = new Input[S, T](
    xBucketer.numBuckets > 1, yBucketer.numBuckets > 1)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[S, T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
    val filtered = inputs.filtered.rdd
    var indexingSeq = Seq[BucketedAttribute[_]]()
    val xBuckets = (if (xBucketer.numBuckets == 1) {
      filtered.mapValues(_ => 0)
    } else {
      val xAttr = inputs.xAttribute.rdd
      indexingSeq = indexingSeq :+ BucketedAttribute(inputs.xAttribute, xBucketer)
      filtered.sortedJoin(xAttr).mapValues { case (_, value) => xBucketer.whichBucket(value) }
    }).asSortedRDD
    val yBuckets = (if (yBucketer.numBuckets == 1) {
      filtered.mapValues(_ => 0)
    } else {
      val yAttr = inputs.yAttribute.rdd
      indexingSeq = indexingSeq :+ BucketedAttribute(inputs.yAttribute, yBucketer)
      filtered.sortedJoin(yAttr).mapValues { case (_, value) => yBucketer.whichBucket(value) }
    }).asSortedRDD
    output(o.xBuckets, xBuckets)
    output(o.yBuckets, yBuckets)
    val xyBuckets = xBuckets.sortedJoin(yBuckets)
    val sampleSize = xBucketer.numBuckets * yBucketer.numBuckets * 1000000
    val sample = xyBuckets.collectFirstNValuesOrSo(sampleSize)
    output(
      o.bucketSizes,
      sample
        .map { case (xB, yB) => ((xB, yB), ()) }
        .groupBy(_._1)
        .toMap
        .mapValues(_.size))
    output(o.indexingSeq, indexingSeq)
  }
}
