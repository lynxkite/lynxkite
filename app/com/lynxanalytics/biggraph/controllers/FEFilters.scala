package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

case class FEVertexAttributeFilter(
    val attributeId: String,
    val valueSpec: String) {

  def toFilteredAttribute(
    implicit manager: MetaGraphManager): FilteredAttribute[_] = {
    toFilteredAttributeFromAttribute(manager.vertexAttribute(attributeId.asUUID))
  }

  private def toFilteredAttributeFromAttribute[T](
    attr: VertexAttribute[T]): FilteredAttribute[T] = {
    implicit val tt = attr.typeTag
    return FilteredAttribute(attr, FEFilters.filterFromSpec(valueSpec))
  }
}

object FEFilters {
  def filter(
    vertexSet: VertexSet, filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    if (filters.isEmpty) return vertexSet
    intersectionEmbedding(filters.map(applyFEFilter)).srcVertexSet
  }

  def embedFilteredVertices(
    base: VertexSet, filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    intersectionEmbedding(base +: filters.map(applyFEFilter))
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter])(
    implicit metaManager: MetaGraphManager): VertexSet = {
    embedFilteredVertices(filtered, moreFilters).srcVertexSet
  }

  private def applyFEFilter(
    filterSpec: FEVertexAttributeFilter)(
      implicit metaManager: MetaGraphManager): VertexSet = {

    val attr = metaManager.vertexAttribute(filterSpec.attributeId.asUUID)
    FEFilters.filteredBaseSet(
      metaManager,
      attr,
      filterSpec.valueSpec)
  }

  private def intersectionEmbedding(
    filteredVss: Seq[VertexSet])(
      implicit metaManager: MetaGraphManager): EdgeBundle = {

    val op = VertexSetIntersection(filteredVss.size)
    op(op.vss, filteredVss).result.firstEmbedding
  }

  def filterFromSpec[T: TypeTag](spec: String): Filter[T] = {
    val negated = spec.startsWith("!")
    val innerSpec = if (negated) spec.drop(1) else spec
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
    if (negated) NotFilter(innerFilter) else innerFilter
  }

  private def filteredBaseSet[T](
    manager: MetaGraphManager,
    attr: VertexAttribute[T],
    spec: String): VertexSet = {

    implicit val tt = attr.typeTag
    val filter = filterFromSpec[T](spec)
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
