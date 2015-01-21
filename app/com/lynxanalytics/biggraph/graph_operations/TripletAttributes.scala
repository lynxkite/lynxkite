package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable
import scala.reflect._
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object TripletMapping extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    // The list of outgoing edges.
    val srcEdges = vertexAttribute[Array[ID]](inputs.src.entity)
    // The list of incoming edges.
    val dstEdges = vertexAttribute[Array[ID]](inputs.dst.entity)
  }
  def fromJson(j: play.api.libs.json.JsValue) = TripletMapping((j \ "sampleSize").as[Int])
}
// A negative sampleSize means no sampling.
case class TripletMapping(sampleSize: Int = -1)
    extends TypedMetaGraphOp[TripletMapping.Input, TripletMapping.Output] {
  import TripletMapping._
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges =
      if (sampleSize >= 0) inputs.edges.rdd.takeFirstNValuesOrSo(sampleSize)
      else inputs.edges.rdd
    val src = inputs.src.rdd
    val bySrc = edges
      .map { case (id, edge) => (edge.src, id) }
      .groupByKey(src.partitioner.get).toSortedRDD
    output(
      o.srcEdges,
      src.sortedLeftOuterJoin(bySrc)
        .mapValues {
          case (_, Some(it)) => it.toArray
          case (_, None) => Array[ID]()
        })

    val dst = inputs.dst.rdd
    val byDst = edges
      .map { case (id, edge) => (edge.dst, id) }
      .groupByKey(dst.partitioner.get).toSortedRDD
    output(
      o.dstEdges,
      dst.sortedLeftOuterJoin(byDst)
        .mapValues {
          case (_, Some(it)) => it.toArray
          case (_, None) => Array[ID]()
        })
  }
}

object VertexToEdgeAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val mapping = vertexAttribute[Array[ID]](vertices)
    val original = vertexAttribute[T](vertices)
    val target = edgeBundle(ignoredSrc, ignoredDst)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val mappedAttribute = edgeAttribute[T](inputs.target.entity)(inputs.original.typeTag)
  }

  def srcAttribute[T](
    attr: Attribute[T], edgeBundle: EdgeBundle)(
      implicit manager: MetaGraphManager): Attribute[T] = {
    import Scripting._
    val mapping = {
      val op = TripletMapping()
      op(op.edges, edgeBundle).result.srcEdges
    }
    val mop = VertexToEdgeAttribute[T]()
    mop(mop.mapping, mapping)(mop.original, attr)(mop.target, edgeBundle).result.mappedAttribute
  }

  def dstAttribute[T](
    attr: Attribute[T], edgeBundle: EdgeBundle)(
      implicit manager: MetaGraphManager): Attribute[T] = {
    import Scripting._
    val mapping = {
      val op = TripletMapping()
      op(op.edges, edgeBundle).result.dstEdges
    }
    val mop = VertexToEdgeAttribute[T]()
    mop(mop.mapping, mapping)(mop.original, attr)(mop.target, edgeBundle).result.mappedAttribute
  }
  def fromJson(j: play.api.libs.json.JsValue) = VertexToEdgeAttribute[Any]()
}
case class VertexToEdgeAttribute[T]()
    extends TypedMetaGraphOp[VertexToEdgeAttribute.Input[T], VertexToEdgeAttribute.Output[T]] {
  import VertexToEdgeAttribute._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val mapping = inputs.mapping.rdd
    val original = inputs.original.rdd
    val target = inputs.target.rdd

    implicit val ct = inputs.original.meta.classTag

    output(
      o.mappedAttribute,
      mapping.sortedJoin(original)
        .flatMap { case (vid, (edges, value)) => edges.map((_, value)) }
        .groupBySortedKey(target.partitioner.get)
        .mapValues(values => values.head))
  }
}

object EdgesForVertices extends OpFromJson {
  class Input(bySource: Boolean) extends MagicInputSignature {
    val vs = vertexSet
    val otherVs = vertexSet
    val tripletMapping = vertexAttribute[Array[ID]](vs)
    val edges = if (bySource) edgeBundle(vs, otherVs) else edgeBundle(otherVs, vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val edges = scalar[Option[Seq[(ID, Edge)]]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = EdgesForVertices(Set(), 1, true)
}
case class EdgesForVertices(vertexIdSet: Set[ID], maxNumEdges: Int, bySource: Boolean)
    extends TypedMetaGraphOp[EdgesForVertices.Input, EdgesForVertices.Output] {
  import EdgesForVertices._
  @transient override lazy val inputs = new Input(bySource)

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    // Do some additional checking on the inputs.
    val tripletMapping = inputs.tripletMapping.entity
    val tripletMappingInstance = tripletMapping.source
    assert(tripletMappingInstance.operation.isInstanceOf[TripletMapping])
    assert(tripletMappingInstance.inputs.edgeBundles('edges) == inputs.edges.entity)
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val restricted = inputs.tripletMapping.rdd.restrictToIdSet(vertexIdSet.toIndexedSeq.sorted)
    val aggregatedIds =
      restricted.aggregate(mutable.Set[ID]())(
        {
          case (set, (id, array)) =>
            if ((set == null) || (set.size + array.size > maxNumEdges)) {
              null
            } else {
              set ++= array
              set
            }
        },
        {
          case (set1, set2) =>
            if ((set1 == null) || (set2 == null)) null
            else {
              set1 ++= set2
              set1
            }
        })
    output(
      o.edges,
      Option(aggregatedIds).map(
        ids => inputs.edges.rdd.restrictToIdSet(ids.toIndexedSeq.sorted).collect.toSeq))
  }
}
