// Buckets vertices by 0, 1, or 2 attributes.
// Used for generating the bucketed visualization.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util._

object VertexBucketGrid extends OpFromJson {
  private val sampleSizeParameter = NewParameter("sampleSize", 50000)
  class Input[S, T](xBucketed: Boolean, yBucketed: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val xAttribute = if (xBucketed) vertexAttribute[S](vertices) else null
    val yAttribute = if (yBucketed) vertexAttribute[T](vertices) else null
    // Providing the filtered vertex set as a separate input avoids having to
    // pull over xAttribute and yAttribute.
    val filtered = vertexSet
    val originalCount = scalar[Long]
  }
  class Output[S, T](implicit instance: MetaGraphOperationInstance, inputs: Input[S, T]) extends MagicOutput(instance) {
    val buckets = scalar[IDBuckets[(Int, Int)]]
    val xBuckets = vertexAttribute[Int](inputs.filtered.entity)
    val yBuckets = vertexAttribute[Int](inputs.filtered.entity)
    // 0, 1, or 2 BucketedAttributes, depending on xBucketed and yBucketed.
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
  def fromJson(j: JsValue) = VertexBucketGrid(
    TypedJson.read[Bucketer[_]](j \ "xBucketer"),
    TypedJson.read[Bucketer[_]](j \ "yBucketer"),
    sampleSizeParameter.fromJson(j))
}
import VertexBucketGrid._
case class VertexBucketGrid[S, T](
    xBucketer: Bucketer[S],
    yBucketer: Bucketer[T],
    // specifies the number of data points to use for estimating the number of
    // elements in buckets. A negative value turns sampling off and all the
    // data points will be used.
    sampleSize: Int)
    extends SparkOperation[Input[S, T], Output[S, T]] {

  @transient override lazy val inputs = new Input[S, T](
    xBucketer.nonEmpty,
    yBucketer.nonEmpty)

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  override def toJson = Json.obj(
    "xBucketer" -> xBucketer.toTypedJson,
    "yBucketer" -> yBucketer.toTypedJson) ++
    sampleSizeParameter.toJson(sampleSize)

  def execute(
      inputDatas: DataSet,
      o: Output[S, T],
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val instance = output.instance
    val filtered = inputs.filtered.rdd
    val filteredPartitioner = filtered.partitioner.get
    var indexingSeq = Seq[BucketedAttribute[_]]()
    val xBuckets =
      if (xBucketer.isEmpty) {
        filtered.mapValues(_ => 0)
      } else {
        val xAttr = inputs.xAttribute.rdd
        implicit val ctx = inputs.xAttribute.data.classTag
        indexingSeq = indexingSeq :+ BucketedAttribute(inputs.xAttribute, xBucketer)
        filtered.sortedJoin(xAttr.sortedRepartition(filteredPartitioner))
          .flatMapOptionalValues { case (_, value) => xBucketer.whichBucket(value) }
      }
    val yBuckets =
      if (yBucketer.isEmpty) {
        filtered.mapValues(_ => 0)
      } else {
        val yAttr = inputs.yAttribute.rdd
        implicit val cty = inputs.yAttribute.data.classTag
        indexingSeq = indexingSeq :+ BucketedAttribute(inputs.yAttribute, yBucketer)
        filtered.sortedJoin(yAttr.sortedRepartition(filteredPartitioner))
          .flatMapOptionalValues { case (_, value) => yBucketer.whichBucket(value) }
      }
    output(o.xBuckets, xBuckets)
    output(o.yBuckets, yBuckets)
    val xyBuckets = xBuckets.sortedJoin(yBuckets)
    val vertices = inputs.vertices.rdd
    val originalCount = inputs.originalCount.value
    output(
      o.buckets,
      RDDUtils.estimateOrPreciseValueCounts(vertices, xyBuckets, originalCount, sampleSize, rc))
    output(o.indexingSeq, indexingSeq)
  }
}
