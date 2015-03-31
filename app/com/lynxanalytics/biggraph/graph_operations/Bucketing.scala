package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Helper class for creating segmentations by an attribute.
case class Bucketing[T: Ordering: reflect.ClassTag](attr: AttributeRDD[T]) {
  // This implementation assumes many small buckets. (#1481)
  private val partitioner = attr.partitioner.get
  private val segToValue = attr.values.distinct.randomNumbered(partitioner)
  private val vToSeg = {
    val valueToSeg = segToValue.map { case (seg, value) => (value, seg) }.toSortedRDD(partitioner)
    val valueToV = attr.map { case (v, value) => (value, v) }.toSortedRDD(partitioner)
    valueToV.sortedJoin(valueToSeg).map { case (value, (v, seg)) => (v, seg) }
  }
  val segments = segToValue.mapValues(_ => ())
  val belongsTo = vToSeg.map { case (v, seg) => Edge(v, seg) }.randomNumbered(partitioner)
}

// Creates a segmentation where each segment represents a distinct value of the attribute.
object StringBucketing extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = StringBucketing()
}
case class StringBucketing()
    extends TypedMetaGraphOp[VertexAttributeInput[String], Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val bucketing = Bucketing(inputs.attr.rdd)
    output(o.segments, bucketing.segments)
    output(o.belongsTo, bucketing.belongsTo)
  }
}

// Creates a segmentation where each segment represents a bucket of the attribute.
object DoubleBucketing {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[Double](vs)
    val min = scalar[Double]
    val max = scalar[Double]
  }
}
// "Spread" is the number of neighboring buckets to also assign each vertex into.
// For example with spread = 2 each vertex will belong to 5 segments.
abstract class DoubleBucketing(spread: Long)
    extends TypedMetaGraphOp[DoubleBucketing.Input, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new DoubleBucketing.Input
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    if (spread > 0) new Segmentation(inputs.vs.entity)
    else new Segmentation(inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }

  def whichBucket(min: Double, max: Double, value: Double): Long

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val min = inputs.min.value
    val max = inputs.max.value
    val buckets = inputs.attr.rdd.flatMapValues { value =>
      val bucket = whichBucket(min, max, value)
      (bucket - spread) to (bucket + spread)
    }
    val bucketing = Bucketing(buckets)
    output(o.segments, bucketing.segments)
    output(o.belongsTo, bucketing.belongsTo)
  }
}

// DoubleBucketing with a fixed number of buckets.
object FixedCountDoubleBucketing extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type =
    FixedCountDoubleBucketing((j \ "bucketCount").as[Long], (j \ "spread").as[Long])
}
case class FixedCountDoubleBucketing(bucketCount: Long, spread: Long)
    extends DoubleBucketing(spread) {
  override def toJson = Json.obj("bucketCount" -> bucketCount, "spread" -> spread)
  def whichBucket(min: Double, max: Double, value: Double): Long = {
    val p = (value - min) / max // 0 to 1 inclusive.
    (bucketCount - 1) min (bucketCount * p).toLong
  }
}

// DoubleBucketing with fixed-size buckets.
object FixedWidthDoubleBucketing extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type =
    FixedWidthDoubleBucketing((j \ "bucketWidth").as[Double], (j \ "spread").as[Long])
}
case class FixedWidthDoubleBucketing(bucketWidth: Double, spread: Long)
    extends DoubleBucketing(spread) {
  override def toJson = Json.obj("bucketWidth" -> bucketWidth, "spread" -> spread)
  def whichBucket(min: Double, max: Double, value: Double): Long = {
    val p = (value - min) / bucketWidth
    p.toLong
  }
}
