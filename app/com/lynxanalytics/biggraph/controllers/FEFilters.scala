package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

case class FEVertexAttributeFilter(
  val attributeId: String,
  val valueSpec: String)

object FEFilters {
  def filter(
    vertexSet: VertexSet, filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): VertexSet = {
    if (filters.isEmpty) return vertexSet
    intersectionEmbedding(filters.map(applyFEFilter)).srcVertexSet
  }

  def embedFilteredVertices(
    base: VertexSet, filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): EdgeBundle = {
    intersectionEmbedding(base +: filters.map(applyFEFilter))
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter])(
    implicit metaManager: MetaGraphManager, dataManager: DataManager): VertexSet = {
    embedFilteredVertices(filtered, moreFilters).srcVertexSet
  }

  private def applyFEFilter(
    filterSpec: FEVertexAttributeFilter)(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): VertexSet = {

    val attr = metaManager.vertexAttribute(filterSpec.attributeId.asUUID)
    attr.rdd.cache()
    FEFilters.filteredBaseSet(
      metaManager,
      attr,
      filterSpec.valueSpec)
  }

  private def intersectionEmbedding(
    filteredVss: Seq[VertexSet])(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): EdgeBundle = {

    val op = VertexSetIntersection(filteredVss.size)
    op(op.vss, filteredVss).result.firstEmbedding
  }

  private def filteredBaseSet[T](
    manager: MetaGraphManager,
    attr: VertexAttribute[T],
    spec: String): VertexSet = {

    val negated = spec.startsWith("!")
    val innerSpec = if (negated) spec.drop(1) else spec
    implicit val tt = attr.typeTag
    val innerFilter: Filter[T] =
      if (typeOf[T] =:= typeOf[String]) {
        OneOf(innerSpec.split(",").toSet)
          .asInstanceOf[Filter[T]]
      } else if (typeOf[T] =:= typeOf[Double]) {
        val doubleFilter = innerSpec match {
          case numberRE(num) => DoubleEQ(num.toDouble)
          case intervalOpenOpenRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLT(b.toDouble))
          case intervalOpenCloseRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLE(b.toDouble))
          case intervalCloseOpenRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLT(b.toDouble))
          case intervalCloseCloseRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLE(b.toDouble))
        }
        doubleFilter.asInstanceOf[Filter[T]]
      } else ???
    val filter = if (negated) NotFilter(innerFilter) else innerFilter
    import Scripting._
    implicit val mm = manager
    val op = VertexAttributeFilter(filter)
    return op(op.attr, attr).result.fvs
  }

  private val numberPattern = "(\\d*(?:\\.\\d*)?)"
  private val numberRE = numberPattern.r
  private val intervalOpenOpenRE = s"\\($numberPattern,$numberPattern\\)".r
  private val intervalOpenCloseRE = s"\\($numberPattern,$numberPattern\\]".r
  private val intervalCloseOpenRE = s"\\[$numberPattern,$numberPattern\\)".r
  private val intervalCloseCloseRE = s"\\[$numberPattern,$numberPattern\\]".r
}
