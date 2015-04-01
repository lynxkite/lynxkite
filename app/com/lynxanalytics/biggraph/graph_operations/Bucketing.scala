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
  val label = segToValue
  val belongsTo = vToSeg.map { case (v, seg) => Edge(v, seg) }.randomNumbered(partitioner)
}

// Creates a segmentation where each segment represents a distinct value of the attribute.
object StringBucketing extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[String]) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, EdgeBundleProperties.partialFunction)
    val label = vertexAttribute[String](segments)
  }
  def fromJson(j: JsValue) = StringBucketing()
}
case class StringBucketing()
    extends TypedMetaGraphOp[VertexAttributeInput[String], StringBucketing.Output] {
  import StringBucketing._
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val bucketing = Bucketing(inputs.attr.rdd)
    output(o.segments, bucketing.segments)
    output(o.label, bucketing.label)
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
  class Output(properties: EdgeBundleProperties)(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, properties)
    val label = vertexAttribute[String](segments)
  }
  def fromJson(j: JsValue) =
    DoubleBucketing((j \ "bucketWidth").as[Double], (j \ "overlap").as[Boolean])
}
case class DoubleBucketing(bucketWidth: Double, overlap: Boolean)
    extends TypedMetaGraphOp[DoubleBucketing.Input, DoubleBucketing.Output] {
  import DoubleBucketing._
  override val isHeavy = true
  @transient override lazy val inputs = new DoubleBucketing.Input
  def outputMeta(instance: MetaGraphOperationInstance) = {
    if (overlap) new Output(EdgeBundleProperties.default)(instance, inputs)
    else new Output(EdgeBundleProperties.partialFunction)(instance, inputs)
  }
  override def toJson = Json.obj("bucketWidth" -> bucketWidth, "overlap" -> overlap)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val bucketStep = if (overlap) bucketWidth / 2 else bucketWidth
    val buckets = inputs.attr.rdd.flatMapValues { value =>
      val bucket = (value / bucketStep).toLong
      if (overlap) (bucket - 1) to bucket
      else Some(bucket)
    }
    val bucketing = Bucketing(buckets)
    output(o.segments, bucketing.segments)
    output(o.label, bucketing.label.mapValues { bucket =>
      val bottom = bucket * bucketStep
      val top = bottom + bucketWidth
      s"$bottom to $top"
    })
    output(o.belongsTo, bucketing.belongsTo)
  }
}
