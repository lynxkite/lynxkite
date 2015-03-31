package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Creates a segmentation that is the cross product of two segmentations.
object CombineSegmentations extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val seg1 = vertexSet
    val seg2 = vertexSet
    val belongsTo1 = edgeBundle(vs, seg1)
    val belongsTo2 = edgeBundle(vs, seg2)
  }
  def fromJson(j: JsValue) = CombineSegmentations()
}
import CombineSegmentations._
case class CombineSegmentations()
    extends TypedMetaGraphOp[Input, Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    new Segmentation(inputs.vs.entity)
  }

  // Creates an ID for the new segmentation from IDs in seg1 and seg2.
  private def newID(a: ID, b: ID) = {
    // IDs generated with randomNumbered have 32 random bits and 32 sequential bits.
    // The chance of collision is very low.
    java.lang.Long.rotateLeft(a, 1) ^ b
  }

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vp = inputs.vs.rdd.partitioner.get
    val belongsTo1 = inputs.belongsTo1.rdd.values.map(e => e.src -> e.dst).toSortedRDD(vp)
    val belongsTo2 = inputs.belongsTo2.rdd.values.map(e => e.src -> e.dst).toSortedRDD(vp)
    val belongsTo = belongsTo1.sortedJoin(belongsTo2).map {
      case (v, (seg1, seg2)) => Edge(v, newID(seg1, seg2))
    }.randomNumbered(vp)
    output(o.segments, belongsTo.values.map(e => e.dst -> ()).toSortedRDD(vp).distinct)
    output(o.belongsTo, belongsTo)
  }
}
