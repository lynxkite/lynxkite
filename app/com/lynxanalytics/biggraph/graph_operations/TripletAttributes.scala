// A "triplet mapping" is a vertex attribute that contains the list of outgoing
// or incoming edge IDs. This file contains operations for creating and using
// such triplet mappings. They are used in GraphDrawingController for building
// diagrams efficiently.

package com.lynxanalytics.biggraph.graph_operations

import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Creates outgoing and incoming triplet mappings.
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
  def fromJson(j: JsValue) = TripletMapping((j \ "sampleSize").as[Int])
}
// A negative sampleSize means no sampling.
case class TripletMapping(sampleSize: Int = -1)
    extends TypedMetaGraphOp[TripletMapping.Input, TripletMapping.Output] {
  import TripletMapping._
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = Json.obj("sampleSize" -> sampleSize)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges =
      if (sampleSize >= 0) inputs.edges.rdd.coalesce(rc).takeFirstNValuesOrSo(sampleSize)
      else inputs.edges.rdd
    val src = inputs.src.rdd
    val bySrc = edges
      .map { case (id, edge) => (edge.src, id) }
      .groupBySortedKey(src.partitioner.get)
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
      .groupBySortedKey(dst.partitioner.get)
    output(
      o.dstEdges,
      dst.sortedLeftOuterJoin(byDst)
        .mapValues {
          case (_, Some(it)) => it.toArray
          case (_, None) => Array[ID]()
        })
  }
}

// Creates outgoing and incoming neighbor mappings.
object NeighborMapping extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    // The list of outgoing edges.
    val srcNeighbors = vertexAttribute[Array[ID]](inputs.src.entity)
    // The list of incoming edges.
    val dstNeighbors = vertexAttribute[Array[ID]](inputs.dst.entity)
  }
  def fromJson(j: JsValue) = NeighborMapping((j \ "sampleSize").as[Int])
}
// A negative sampleSize means no sampling.
case class NeighborMapping(sampleSize: Int = -1)
    extends TypedMetaGraphOp[NeighborMapping.Input, NeighborMapping.Output] {
  import NeighborMapping._
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = Json.obj("sampleSize" -> sampleSize)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges =
      if (sampleSize >= 0) inputs.edges.rdd.coalesce(rc).takeFirstNValuesOrSo(sampleSize)
      else inputs.edges.rdd
    val src = inputs.src.rdd
    val bySrc = edges
      .map { case (_, edge) => (edge.src, edge.dst) }
      .groupBySortedKey(src.partitioner.get)
    output(
      o.srcNeighbors,
      src.sortedLeftOuterJoin(bySrc)
        .mapValues {
          case (_, Some(it)) => it.toSet.toArray
          case (_, None) => Array[ID]()
        })

    val dst = inputs.dst.rdd
    val byDst = edges
      .map { case (_, edge) => (edge.dst, edge.src) }
      .groupBySortedKey(dst.partitioner.get)
    output(
      o.dstNeighbors,
      dst.sortedLeftOuterJoin(byDst)
        .mapValues {
          case (_, Some(it)) => it.toSet.toArray
          case (_, None) => Array[ID]()
        })
  }
}

// Pushes a vertex attribute to the edges going from/to the vertex.
object VertexToEdgeAttribute extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    // The list of edge IDs that belong to the vertex.
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
  def fromJson(j: JsValue) = VertexToEdgeAttribute()
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

// Returns a small set of edges given a small set of vertices and a triplet mapping.
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
  def fromJson(j: JsValue) = EdgesForVertices(
    (j \ "vertexIdSet").as[Set[ID]],
    (j \ "maxNumEdges").as[Int],
    (j \ "bySource").as[Boolean])
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
    assert(tripletMappingInstance.operation.isInstanceOf[TripletMapping],
      "tripletMapping is not a TripletMapping")
    assert(tripletMappingInstance.inputs.edgeBundles('edges) == inputs.edges.entity,
      s"tripletMapping is for ${tripletMappingInstance.inputs.edgeBundles('edges)}" +
        s" instead of ${inputs.edges.entity}")
    new Output()(instance, inputs)
  }

  override def toJson = Json.obj(
    "vertexIdSet" -> vertexIdSet,
    "maxNumEdges" -> maxNumEdges,
    "bySource" -> bySource)

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
