package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util._

object VertexBucketGrid extends OpFromJson {
  class Input[S, T](xBucketed: Boolean, yBucketed: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val xAttribute = if (xBucketed) vertexAttribute[S](vertices) else null
    val yAttribute = if (yBucketed) vertexAttribute[T](vertices) else null
    val filtered = vertexSet
    val originalCount = scalar[Long]
  }
  class Output[S, T](implicit instance: MetaGraphOperationInstance,
                     inputs: Input[S, T]) extends MagicOutput(instance) {
    val buckets = scalar[IDBuckets[(Int, Int)]]
    val xBuckets = vertexAttribute[Int](inputs.filtered.entity)
    val yBuckets = vertexAttribute[Int](inputs.filtered.entity)
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
  def fromJson(j: JsValue) = VertexBucketGrid(
    TypedJson.read[Bucketer[_]](j \ "xBucketer"),
    TypedJson.read[Bucketer[_]](j \ "yBucketer"))
}
import VertexBucketGrid._
case class VertexBucketGrid[S, T](xBucketer: Bucketer[S],
                                  yBucketer: Bucketer[T])
    extends TypedMetaGraphOp[Input[S, T], Output[S, T]] {

  @transient override lazy val inputs = new Input[S, T](
    xBucketer.nonEmpty, yBucketer.nonEmpty)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  override def toJson = Json.obj("xBucketer" -> xBucketer.toTypedJson, "yBucketer" -> yBucketer.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Output[S, T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
    val filtered = inputs.filtered.rdd
    var indexingSeq = Seq[BucketedAttribute[_]]()
    val xBuckets = if (xBucketer.isEmpty) {
      filtered.mapValues(_ => 0)
    } else {
      val xAttr = inputs.xAttribute.rdd
      indexingSeq = indexingSeq :+ BucketedAttribute(inputs.xAttribute, xBucketer)
      filtered.sortedJoin(xAttr).flatMapValues { case (_, value) => xBucketer.whichBucket(value) }
    }
    val yBuckets = if (yBucketer.isEmpty) {
      filtered.mapValues(_ => 0)
    } else {
      val yAttr = inputs.yAttribute.rdd
      indexingSeq = indexingSeq :+ BucketedAttribute(inputs.yAttribute, yBucketer)
      filtered.sortedJoin(yAttr).flatMapValues { case (_, value) => yBucketer.whichBucket(value) }
    }
    output(o.xBuckets, xBuckets)
    output(o.yBuckets, yBuckets)
    val xyBuckets = xBuckets.sortedJoin(yBuckets)
    val vertices = inputs.vertices.rdd
    val originalCount = inputs.originalCount.value
    output(
      o.buckets,
      RDDUtils.estimateValueCounts(vertices, xyBuckets, originalCount, 50000))
    output(o.indexingSeq, indexingSeq)
  }
}
