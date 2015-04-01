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
  def fromJson(j: JsValue) = StringBucketing()
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
// Bucketing starts from 0 and each bucket is the size of "bucketWidth". If
// "overlap" is true, buckets will overlap their neighbors, and each vertex will
// belong to 2 segments.
object DoubleBucketing extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[Double](vs)
  }
  def fromJson(j: JsValue) =
    DoubleBucketing((j \ "bucketWidth").as[Double], (j \ "overlap").as[Boolean])
}
case class DoubleBucketing(bucketWidth: Double, overlap: Boolean)
    extends TypedMetaGraphOp[DoubleBucketing.Input, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new DoubleBucketing.Input
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    if (overlap) new Segmentation(inputs.vs.entity)
    else new Segmentation(inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }
  override def toJson = Json.obj("bucketWidth" -> bucketWidth, "overlap" -> overlap)

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val buckets = inputs.attr.rdd.flatMapValues { value =>
      val bucket = (value / bucketWidth).toLong
      if (overlap) (bucket - 1) to bucket
      else Some(bucket)
    }
    val bucketing = Bucketing(buckets)
    output(o.segments, bucketing.segments)
    output(o.belongsTo, bucketing.belongsTo)
  }
}
