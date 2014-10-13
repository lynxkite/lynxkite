package com.lynxanalytics.biggraph.graph_operations

import java.util.UUID
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class BucketedAttribute[T](
    attribute: VertexAttribute[T],
    bucketer: Bucketer[T]) {

  def toHistogram(
    filtered: VertexSet)(
      implicit manager: MetaGraphManager): graph_operations.AttributeHistogram.Output = {
    val cop = graph_operations.CountVertices()
    val originalCount = cop(cop.vertices, attribute.vertexSet).result.count
    val op = graph_operations.AttributeHistogram[T](bucketer)
    op(op.attr, attribute)(op.filtered, filtered)(op.originalCount, originalCount).result
  }
}

case class FilteredAttribute[T](
  attribute: VertexAttribute[T],
  filter: Filter[T])

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
  filtered: VertexSet,
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
  // Filtering in a vertex view has to be a conjunction of per attribute filters.
  filters: Seq[FilteredAttribute[_]]) extends Serializable
object VertexView {
  def fromDiagram(diagram: Scalar[_])(implicit dataManager: DataManager): VertexView = {
    val indexerInstance = diagram.source
    assert(indexerInstance.operation.isInstanceOf[VertexBucketGrid[_, _]],
      s"$indexerInstance is not a VertexBucketGrid")
    val indexingSeq =
      indexerInstance.outputs.scalars('indexingSeq).runtimeSafeCast[Seq[BucketedAttribute[_]]].value
    val vertexSet = indexerInstance.inputs.vertexSets('vertices)
    val filtered = indexerInstance.inputs.vertexSets('filtered)
    val filters: Seq[FilteredAttribute[_]] = if (filtered.gUID == vertexSet.gUID) {
      Seq()
    } else {
      val intersectionInstance = filtered.source
      intersectionInstance.inputs.vertexSets.values.map { vs =>
        val filterInstance = vs.source
        filterInstance.outputs.scalars('filteredAttribute).value.asInstanceOf[FilteredAttribute[_]]
      }.toSeq
    }
    VertexView(vertexSet, filtered, indexingSeq, filters)
  }
}
