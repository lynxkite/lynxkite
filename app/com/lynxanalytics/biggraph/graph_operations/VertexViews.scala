package com.lynxanalytics.biggraph.graph_operations

import java.util.UUID
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class BucketedAttribute[T](
  attribute: VertexAttribute[T],
  bucketer: Bucketer[T])

case class FilteredAttribute[T](
  attribute: VertexAttribute[T],
  filter: Filter[T])

case class VertexView(
  vertexSet: VertexSet,
  filtered: VertexSet,
  // The indexing of a vertex view happens as a "product" of per attribute bucketers. This means
  // that the final index of a vertex v is:
  // indexingSeq(0).whichBucket(v) + indexingSeq(0).numBuckets * (
  //   indexingSeq(1).whichBucket(v) + indexingSeq(1).numBuckets * (
  //     ...
  //               indexingSeq(n-1).whichBucket(v)
  //     ...
  //   )
  // )
  indexingSeq: Seq[BucketedAttribute[_]],
  // Filtering in a vertex view has to be a conjunction of per attribute filters.
  filters: Seq[FilteredAttribute[_]]) extends Serializable
object VertexView {
  def fromDiagram(diagram: Scalar[_])(implicit dataManager: DataManager): VertexView = {
    val indexerInstance = diagram.source
    val indexingSeq =
      indexerInstance.outputs.scalars('indexingSeq).runtimeSafeCast[Seq[BucketedAttribute[_]]].value
    val vertexSet = indexerInstance.outputs.vertexSets('vertexSet)
    val filtered = indexerInstance.outputs.vertexSets('filtered)
    val filters: Seq[FilteredAttribute[_]] = if (filtered.gUID == vertexSet.gUID) {
      Seq()
    } else {
      val intersectionInstance = filtered.source
      intersectionInstance.inputs.vertexSets.values.map { vs =>
        val filterInstance = vs.source
        vs.outputs.scalars('filteredAttribute).value.asInstanceOf[FilteredAttribute[_]]
      }
    }
    null
  }
}
