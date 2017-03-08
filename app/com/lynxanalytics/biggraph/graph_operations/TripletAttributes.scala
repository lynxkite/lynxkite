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

object EdgesAndNeighbors {
  def empty = EdgesAndNeighbors(Array[ID](), Array[ID]())
}

// A wrapper class to hold the egde and neighbor IDs of a vertex. We use long arrays
// for performance reasons.
case class EdgesAndNeighbors(eids: Array[ID], nids: Array[ID]) {
  assert(eids.size == nids.size,
    s"The number of edges ${eids.size} does not match the number of neighbors ${nids.size}.")

  def map[B](f: ((ID, ID)) => B): Iterable[B] = {
    (eids zip nids).map(f)
  }

  def size: Long = eids.size
}

// Creates outgoing and incoming edge mappings of edge and corresponding neighbor IDs.
object EdgeAndNeighborMapping extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    // The list of outgoing edges and neighbors.
    val srcEdges = vertexAttribute[EdgesAndNeighbors](inputs.src.entity)
    // The list of incoming edges and neighbors.
    val dstEdges = vertexAttribute[EdgesAndNeighbors](inputs.dst.entity)
  }
  def fromJson(j: JsValue) = EdgeAndNeighborMapping((j \ "sampleSize").as[Int])
}
// A negative sampleSize means no sampling.
case class EdgeAndNeighborMapping(sampleSize: Int = -1)
    extends TypedMetaGraphOp[EdgeAndNeighborMapping.Input, EdgeAndNeighborMapping.Output] {
  import EdgeAndNeighborMapping._
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
      .map { case (id, edge) => (edge.src, (id, edge.dst)) }
      .groupBySortedKey(src.partitioner.get)
    output(
      o.srcEdges,
      src.sortedLeftOuterJoin(bySrc)
        .mapValues {
          case (_, Some(it)) => EdgesAndNeighbors(it.map(_._1).toArray, it.map(_._2).toArray)
          case (_, None) => EdgesAndNeighbors.empty
        })

    val dst = inputs.dst.rdd
    val byDst = edges
      .map { case (id, edge) => (edge.dst, (id, edge.src)) }
      .groupBySortedKey(dst.partitioner.get)
    output(
      o.dstEdges,
      dst.sortedLeftOuterJoin(byDst)
        .mapValues {
          case (_, Some(it)) => EdgesAndNeighbors(it.map(_._1).toArray, it.map(_._2).toArray)
          case (_, None) => EdgesAndNeighbors.empty
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
@deprecated("EdgesForVertices is deprecated, use EdgesForVerticesFromEdgesAndNeighbors", "1.13.0")
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

// Returns a small set of edges given a small set of vertices and a triplet mapping.
object EdgesForVerticesFromEdgesAndNeighbors extends OpFromJson {
  class Input(bySource: Boolean) extends MagicInputSignature {
    val vs = vertexSet
    val otherVs = vertexSet
    val mapping = vertexAttribute[EdgesAndNeighbors](vs)
    val edges = if (bySource) edgeBundle(vs, otherVs) else edgeBundle(otherVs, vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val edges = scalar[Option[Seq[(ID, Edge)]]]
  }
  def fromJson(j: JsValue) = EdgesForVerticesFromEdgesAndNeighbors(
    (j \ "srcIdSet").as[Set[ID]],
    (j \ "dstIdSet").as[Option[Set[ID]]],
    (j \ "maxNumEdges").as[Int],
    (j \ "bySource").as[Boolean])
}
case class EdgesForVerticesFromEdgesAndNeighbors(
  srcIdSet: Set[ID],
  dstIdSet: Option[Set[ID]], // Filter the edges by dst too if set.
  maxNumEdges: Int,
  bySource: Boolean)
    extends TypedMetaGraphOp[EdgesForVerticesFromEdgesAndNeighbors.Input, EdgesForVerticesFromEdgesAndNeighbors.Output] {
  import EdgesForVerticesFromEdgesAndNeighbors._
  @transient override lazy val inputs = new Input(bySource)

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    // Do some additional checking on the inputs.
    val mapping = inputs.mapping.entity
    val mappingInstance = mapping.source
    assert(mappingInstance.operation.isInstanceOf[EdgeAndNeighborMapping],
      "tripletMapping is not a EdgeAndNeighborMapping")
    assert(mappingInstance.inputs.edgeBundles('edges) == inputs.edges.entity,
      s"tripletMapping is for ${mappingInstance.inputs.edgeBundles('edges)}" +
        s" instead of ${inputs.edges.entity}")
    new Output()(instance, inputs)
  }

  override def toJson = Json.obj(
    "srcIdSet" -> srcIdSet,
    "dstIdSet" -> dstIdSet,
    "maxNumEdges" -> maxNumEdges,
    "bySource" -> bySource)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val restricted = inputs.mapping.rdd.restrictToIdSet(srcIdSet.toIndexedSeq.sorted)
    val aggregatedEdges =
      restricted.aggregate(mutable.Set[(ID, Edge)]())(
        {
          case (set, (srcId, edgesAndNeighbors)) =>
            val it = edgesAndNeighbors.map { case (edgeId, dstId) => edgeId -> Edge(srcId, dstId) }
            val byDst = if (dstIdSet.isDefined) it.filter(dstIdSet.get contains _._2.dst) else it
            if ((set == null) || (set.size + byDst.size > maxNumEdges)) {
              null
            } else {
              set ++= byDst
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
      Option(aggregatedEdges).map(_.toIndexedSeq))
  }
}
