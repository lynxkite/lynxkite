// Merges vertices that match on an attribute.
package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark

import scala.math.Ordering
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object MergeVertices extends OpFromJson {
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[_]) extends MagicOutput(instance) {

    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, EdgeBundleProperties.partialFunction)
    val representative = edgeBundle(segments, inputs.vs.entity, EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = MergeVertices()
}
import MergeVertices._
case class MergeVertices[T]() extends TypedMetaGraphOp[VertexAttributeInput[T], Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  // Non scaling merge method for any type.
  def groupByMerge(
    attr: spark.rdd.RDD[(ID, T)],
    o: Output,
    output: OutputBuilder)(implicit rc: RuntimeContext,
                           inputDatas: DataSet): Unit = {
    implicit val ct = inputs.attr.data.classTag
    val partitioner = attr.partitioner.get
    val byAttr = attr.map(_.swap)
    val matching = byAttr
      .groupByKey(partitioner)
      .mapPartitionsWithIndex {
        case (pid, it) =>
          val rnd = new Random(pid)
          it.map {
            case (attr, vertices) =>
              (vertices.drop(rnd.nextInt(vertices.size)).head, vertices)
          }
      }
      .sortUnique(partitioner)
    output(o.segments, matching.mapValues(_ => ()))
    output(o.representative, matching.mapValuesWithKeys { case (key, _) => Edge(key, key) })
    output(
      o.belongsTo,
      matching.flatMap {
        case (groupId, vertices) =>
          vertices.map(v => v -> Edge(v, groupId))
      }.sortUnique(partitioner))
  }

  // Scalable merge method for types with ordering.
  def efficientMerge[V: Ordering: ClassTag](
    attr: spark.rdd.RDD[(ID, V)],
    o: Output,
    output: OutputBuilder)(implicit rc: RuntimeContext,
                           inputDatas: DataSet): Unit = {
    val partitioner = attr.partitioner.get

    val byAttr = attr.map(_.swap)
    val segmentIdToAttr = byAttr.map { case (key, _) => key }.distinct.randomNumbered(partitioner)
    segmentIdToAttr.persist(spark.storage.StorageLevel.DISK_ONLY)
    val attrToSegmentId = segmentIdToAttr.map(_.swap).sortUnique(partitioner)
    // Join the segment ids with the original vertex ids using the attribute.
    val matching = HybridRDD.of(byAttr, partitioner, even = true).lookup(attrToSegmentId).map {
      case (_, (vid, groupId)) => vid -> Edge(vid, groupId)
    }.sortUnique(partitioner)

    output(o.segments, segmentIdToAttr.mapValues(_ => ()))
    output(o.representative, segmentIdToAttr.mapValuesWithKeys { case (key, _) => Edge(key, key) })
    output(o.belongsTo, matching)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attr.data.typeTag
    implicit val runtimeContext = rc
    val attr = inputs.attr.rdd

    if (typeOf[T] =:= typeOf[Double]) {
      efficientMerge(attr.asInstanceOf[spark.rdd.RDD[(ID, Double)]], o, output)
    } else if (typeOf[T] =:= typeOf[Long]) {
      efficientMerge(attr.asInstanceOf[spark.rdd.RDD[(ID, Long)]], o, output)
    } else if (typeOf[T] =:= typeOf[String]) {
      efficientMerge(attr.asInstanceOf[spark.rdd.RDD[(ID, String)]], o, output)
    } else {
      groupByMerge(attr, o, output)
    }
  }
}
