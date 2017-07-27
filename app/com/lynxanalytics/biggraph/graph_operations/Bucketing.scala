// Creates a segmentation where each segment represents a bucket of an attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.SortedRDD

// Helper class for creating segmentations by an attribute.
case class Bucketing[T: Ordering: reflect.ClassTag](
    attrIdsToBuckets: SortedRDD[ID, T], even: Boolean = true)(implicit rc: RuntimeContext) {
  private val partitioner = rc.partitionerForNRows(if (even) {
    RDDUtils.countApproxEvenRDD(attrIdsToBuckets)
  } else {
    attrIdsToBuckets.count
  })
  private val segToValue = attrIdsToBuckets.values.distinct.randomNumbered(partitioner)
  private val vToSeg = {
    val valueToSeg = segToValue.map(_.swap).sortUnique(partitioner)
    HybridRDD.of(attrIdsToBuckets.map(_.swap), partitioner, even)
      .lookup(valueToSeg)
      .map { case (value, (v, seg)) => (v, seg) }
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
    implicit val runtimeContext = rc
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
    // The start and end of intervals for each segment.
    val bottom = vertexAttribute[Double](segments)
    val top = vertexAttribute[Double](segments)
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
    implicit val runtimeContext = rc
    val bucketStep = if (overlap) bucketWidth / 2 else bucketWidth
    val buckets = inputs.attr.rdd.flatMapValues { value =>
      val bucket = (value / bucketStep).floor.round
      if (overlap) (bucket - 1) to bucket
      else Some(bucket)
    }
    val bucketing = Bucketing(buckets)
    output(o.segments, bucketing.segments)
    output(o.bottom, bucketing.label.mapValues { bucket => bucket * bucketStep })
    output(o.top, bucketing.label.mapValues { bucket => bucket * bucketStep + bucketWidth })
    output(o.belongsTo, bucketing.belongsTo)
  }
}

// Creates a segmentation where each vertex is treated as an interval defined
// by two of its attributes. Each segment represents an interval, and
// all the vertices with intersecting intervals are counted to the segment.
// Bucketing starts from 0 and each bucket is the size of "bucketWidth". If
// "overlap" is true, buckets will overlap their neighbors.
object IntervalBucketing extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val beginAttr = vertexAttribute[Double](vs)
    val endAttr = vertexAttribute[Double](vs)
  }
  class Output(properties: EdgeBundleProperties)(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, properties)
    // The start and end of intervals for each segment.
    val bottom = vertexAttribute[Double](segments)
    val top = vertexAttribute[Double](segments)
  }
  def fromJson(j: JsValue) =
    IntervalBucketing((j \ "bucketWidth").as[Double], (j \ "overlap").as[Boolean])
}
case class IntervalBucketing(bucketWidth: Double, overlap: Boolean)
    extends TypedMetaGraphOp[IntervalBucketing.Input, IntervalBucketing.Output] {
  import IntervalBucketing._
  override val isHeavy = true
  @transient override lazy val inputs = new IntervalBucketing.Input
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
    implicit val runtimeContext = rc
    val bucketStep = if (overlap) bucketWidth / 2 else bucketWidth
    // vertexid -> (begin, end):
    val inputIntervals = inputs.beginAttr.rdd.sortedJoin(inputs.endAttr.rdd)
    val buckets = inputIntervals.flatMapValues {
      case (begin, end) =>
        val beginBucket = (begin / bucketStep).floor.round
        val endBucket = (end / bucketStep).floor.round
        if (overlap) (beginBucket - 1) to endBucket
        else beginBucket to endBucket
    }
    val bucketing = Bucketing(buckets)
    output(o.segments, bucketing.segments)
    output(o.bottom, bucketing.label.mapValues { bucket => bucket * bucketStep })
    output(o.top, bucketing.label.mapValues { bucket => bucket * bucketStep + bucketWidth })
    output(o.belongsTo, bucketing.belongsTo)
  }
}
