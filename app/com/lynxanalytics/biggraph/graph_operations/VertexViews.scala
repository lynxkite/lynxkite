package com.lynxanalytics.biggraph.graph_operations

import java.util.UUID
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.IDBuckets
import com.lynxanalytics.biggraph.spark_util.Implicits._

trait AttributeFromGUID[T] extends Serializable {
  protected val attributeGUID: UUID
  def attribute(implicit manager: MetaGraphManager) =
    manager.vertexAttribute(attributeGUID).asInstanceOf[Attribute[T]]
  def isEmpty = attributeGUID == null
  def nonEmpty = !isEmpty
}

class BucketedAttribute[T] private (
    protected val attributeGUID: UUID,
    val bucketer: Bucketer[T]) extends AttributeFromGUID[T] {

  def toHistogram(
    filtered: VertexSet)(
      implicit manager: MetaGraphManager): graph_operations.AttributeHistogram.Output = {
    val cop = graph_operations.CountVertices()
    val originalCount = cop(cop.vertices, attribute.vertexSet).result.count
    val op = graph_operations.AttributeHistogram[T](bucketer)
    op(op.attr, attribute)(op.filtered, filtered)(op.originalCount, originalCount).result
  }
}
object BucketedAttribute {
  def apply[T](attribute: Attribute[T], bucketer: Bucketer[T]): BucketedAttribute[T] =
    new BucketedAttribute(attribute.gUID, bucketer)
  def emptyBucketedAttribute: BucketedAttribute[Nothing] =
    new BucketedAttribute[Nothing](null, EmptyBucketer())
}

class FilteredAttribute[T] private (
    protected val attributeGUID: UUID,
    val filter: Filter[T]) extends AttributeFromGUID[T] {
}
object FilteredAttribute {
  def apply[T](attribute: Attribute[T], filter: Filter[T]): FilteredAttribute[T] =
    new FilteredAttribute(attribute.gUID, filter)
}

/*
 * The VertexView class is used to define how a view of some vertices or vertex sets were created.
 *
 * This is useful when we want to connect edges to nodes in this view as then we need to re-perform
 * the same operations on the end-vertices of the edges that we consider.
 *
 * This basically contains three bits of information:
 *  - the base vertex set used
 *  - the filters applied to restrict the vertices
 *  - the process used to assign a bucket index to a vertex
 */
case class VertexView(
  vertexSet: VertexSet,
  // The indexing of a vertex view happens as a "product" of per attribute bucketers. This means
  // that the final index of a vertex v is:
  // indexingSeq(n-1).whichBucket(v) + indexingSeq(n-1).numBuckets * (
  //   indexingSeq(n-2).whichBucket(v) + indexingSeq(n-2).numBuckets * (
  //     ...
  //               indexingSeq(0).whichBucket(v)
  //     ...
  //   )
  // )
  indexingSeq: Seq[BucketedAttribute[_]],
  // In case the set of vertices used to create this view is small, then this is set to a local map
  // telling the bucket index of each vertex used. Otherwise this is None.
  vertexIndices: Option[Map[ID, Int]],
  // Filtering in a vertex view has to be a conjunction of per attribute filters.
  filters: Seq[FilteredAttribute[_]]) extends Serializable
object VertexView {
  def fromDiagram(diagram: Scalar[_])(
    implicit metaManager: MetaGraphManager, dataManager: DataManager): VertexView = {

    val indexerInstance = diagram.source
    assert(
      indexerInstance.operation.isInstanceOf[VertexBucketGrid[_, _]] ||
        indexerInstance.operation.isInstanceOf[SampledView],
      s"$indexerInstance is neither a VertexBucketGrid nor a SampledView")

    val indexingSeq =
      indexerInstance.outputs.scalars('indexingSeq).runtimeSafeCast[Seq[BucketedAttribute[_]]].value

    val vertexSet = indexerInstance.inputs.vertexSets('vertices)

    val vertexIndices = if (indexerInstance.operation.isInstanceOf[SampledView]) {
      Some(indexerInstance.outputs.scalars('vertexIndices).runtimeSafeCast[Map[ID, Int]].value)
    } else {
      assert(
        indexerInstance.operation.isInstanceOf[VertexBucketGrid[_, _]],
        s"$indexerInstance is neither a VertexBucketGrid nor a SampledView")
      val idBuckets = indexerInstance.outputs.scalars('buckets)
        .runtimeSafeCast[IDBuckets[(Int, Int)]].value
      // This is pretty terrible. TODO: make vertex bucket grid generate indexes directly, not
      // x-y coordinates. If we are at it, remove duplications between indexer and
      // vertexbucketgrid.
      val ySize = if (indexingSeq.size == 2) indexingSeq(1).bucketer.numBuckets else 1
      Option(idBuckets.sample).map(_.map { case (id, (x, y)) => (id, x * ySize + y) }.toMap)
    }

    val filtered = indexerInstance.inputs.vertexSets('filtered)
    val filtersFromInputs: Seq[FilteredAttribute[_]] = if (filtered.gUID == vertexSet.gUID) {
      Seq()
    } else {
      val intersectionInstance = filtered.source
      intersectionInstance.inputs.vertexSets.values.map { vs =>
        val filterInstance = vs.source
        filterInstance.outputs.scalars('filteredAttribute).value.asInstanceOf[FilteredAttribute[_]]
      }.toSeq
    }
    val filters = if (indexerInstance.operation.isInstanceOf[SampledView]) {
      // For sampled view we need to explicitly add an id filter here. Without this if
      // we go with huge edge set mode, that is going through all edges and checking which one
      // matches, we will find ones that we shouldn't.
      val iaaop = graph_operations.IdAsAttribute()
      val idAttr = iaaop(iaaop.vertices, vertexSet).result.vertexIds
      filtersFromInputs :+ FilteredAttribute(idAttr, OneOf(vertexIndices.get.keySet))
    } else {
      assert(
        indexerInstance.operation.isInstanceOf[VertexBucketGrid[_, _]],
        s"$indexerInstance is neither a VertexBucketGrid nor a SampledView")
      filtersFromInputs
    }

    VertexView(vertexSet, indexingSeq, vertexIndices, filters)
  }
}
