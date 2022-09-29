// Creates a segmentation that is the cross product of two segmentations.
package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

object CombineSegmentations extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val seg1 = vertexSet
    val seg2 = vertexSet
    val belongsTo1 = edgeBundle(vs, seg1)
    val belongsTo2 = edgeBundle(vs, seg2)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    private val EBP = EdgeBundleProperties // Short alias.
    val belongsTo = {
      val p1 = inputs.belongsTo1.entity.properties
      val p2 = inputs.belongsTo2.entity.properties
      val properties =
        if (p1.isFunction && p2.isFunction) EBP.partialFunction
        else EBP.default
      edgeBundle(inputs.vs.entity, segments, properties)
    }
    val origin1 = edgeBundle(segments, inputs.seg1.entity, EBP.partialFunction)
    val origin2 = edgeBundle(segments, inputs.seg2.entity, EBP.partialFunction)
  }
  def fromJson(j: JsValue) = CombineSegmentations()
}
import CombineSegmentations._
case class CombineSegmentations()
    extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vp = inputs.vs.rdd.partitioner.get
    val belongsTo1 = inputs.belongsTo1.rdd.values.map(e => e.src -> e.dst)
    val belongsTo2 = inputs.belongsTo2.rdd.values.map(e => e.src -> e.dst)
    // Unsorted join, because multiple values are expected on both sides.
    val vToSeg12 = belongsTo1.join(belongsTo2)
    // Generate random IDs for the new segments.
    val segToSeg12 = vToSeg12.values.distinct.randomNumbered(vp)
    val seg12ToSeg = segToSeg12.map(_.swap).sortUnique(vp)
    val seg12ToV = vToSeg12.map(_.swap).sort(vp)
    val vToSeg = seg12ToV.sortedJoin(seg12ToSeg).values
    output(o.segments, segToSeg12.mapValues(_ => ()))
    output(o.belongsTo, vToSeg.map { case (v, seg) => Edge(v, seg) }.randomNumbered(vp))
    output(
      o.origin1,
      segToSeg12.map {
        case (seg, (seg1, seg2)) => Edge(seg, seg1)
      }.randomNumbered(vp))
    output(
      o.origin2,
      segToSeg12.map {
        case (seg, (seg1, seg2)) => Edge(seg, seg2)
      }.randomNumbered(vp))
  }
}
