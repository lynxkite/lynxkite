package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Creates a segmentation by applying a Bucketer to an attribute.
object Bucketing extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type =
    Bucketing(TypedJson.read[Bucketer[_]](j \ "bucketer"))
}
case class Bucketing[T](bucketer: Bucketer[T])
    extends TypedMetaGraphOp[VertexAttributeInput[T], Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(inputs.vs.entity, EdgeBundleProperties.partialFunction)
  }
  override def toJson = Json.obj("bucketer" -> bucketer.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.meta.classTag
    val partitioner = inputs.attr.rdd.partitioner.get
    val segToBucket = rc.parallelize(0L until bucketer.numBuckets).randomNumbered(rc.defaultPartitioner)
    val vToBucket = inputs.attr.rdd.flatMapValues(a => bucketer.whichBucket(a).map(_.toLong))
    val vToSeg = if (bucketer.numBuckets < 100000) {
      // Few, large buckets.
      val bucketToV = vToBucket.collect.map { case (seg, bucket) => (bucket, seg) }.toMap
      vToBucket.mapValues(bucketToV(_))
    } else {
      // Many small buckets.
      val bucketToSeg = segToBucket.map { case (seg, bucket) => (bucket, seg) }.toSortedRDD(partitioner)
      val bucketToV = vToBucket.map { case (v, bucket) => (bucket, v) }.toSortedRDD
      bucketToV.sortedJoin(bucketToSeg).map { case (bucket, (v, seg)) => (v, seg) }
    }
    output(o.segments, segToBucket.mapValues(_ => ()))
    output(o.belongsTo, vToSeg.map { case (v, seg) => Edge(v, seg) }.randomNumbered(partitioner))
  }
}
