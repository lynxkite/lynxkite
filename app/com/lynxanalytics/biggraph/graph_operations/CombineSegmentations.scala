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

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vp = inputs.vs.rdd.partitioner.get
    val belongsTo1 = inputs.belongsTo1.rdd.values.map(e => e.src -> e.dst).toSortedRDD(vp)
    val belongsTo2 = inputs.belongsTo2.rdd.values.map(e => e.src -> e.dst).toSortedRDD(vp)
    val vToSeg12 = belongsTo1.sortedJoin(belongsTo2)
    // Generate random IDs for the new segments.
    val segToSeg12 = vToSeg12.values.distinct.randomNumbered(vp)
    val seg12ToSeg = segToSeg12.map { case (seg, seg12) => (seg12, seg) }.toSortedRDD(vp)
    val seg12ToV = vToSeg12.map { case (v, seg12) => (seg12, v) }.toSortedRDD(vp)
    val vToSeg = seg12ToV.sortedJoin(seg12ToSeg).values
    output(o.segments, segToSeg12.mapValues(_ => ()))
    output(o.belongsTo, vToSeg.map { case (v, seg) => Edge(v, seg) }.randomNumbered(vp))
  }
}
