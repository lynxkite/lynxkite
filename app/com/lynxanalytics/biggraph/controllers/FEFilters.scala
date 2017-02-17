// Parses the filters set on the UI and creates Filters and FilteredAttributes.
package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

case class FEVertexAttributeFilter(
    val attributeId: String,
    val valueSpec: String) {

  def attribute(
    implicit manager: MetaGraphManager): Attribute[_] = {
    manager.attribute(attributeId.asUUID)
  }

  def toFilteredAttribute(
    implicit manager: MetaGraphManager): FilteredAttribute[_] = {
    toFilteredAttributeFromAttribute(attribute)
  }

  private def toFilteredAttributeFromAttribute[T](
    attr: Attribute[T]): FilteredAttribute[T] = {
    implicit val tt = attr.typeTag
    return FilteredAttribute(attr, FEFilters.filterFromSpec(valueSpec))
  }
}

object FEFilters {
  def filter(
    vertexSet: VertexSet, filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    filterFA(vertexSet, filters.map(_.toFilteredAttribute))
  }

  def filterFA(
    vertexSet: VertexSet, filters: Seq[FilteredAttribute[_]])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    for (f <- filters) {
      assert(f.attribute.vertexSet == vertexSet,
        s"Filter $f does not match vertex set $vertexSet")
    }
    if (filters.isEmpty) return vertexSet
    intersectionEmbedding(filters.map(applyFilter(_))).srcVertexSet
  }

  def localFilter(
    vertices: Set[ID], filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): Set[ID] = {
    filters.foldLeft(vertices) { (vs, filter) =>
      localFilter(vs, filter.attribute, filter.valueSpec)
    }
  }

  def localFilter[T](
    vertices: Set[ID], attr: Attribute[T], spec: String)(
      implicit metaManager: MetaGraphManager, dataManager: DataManager): Set[ID] = {
    implicit val tt = attr.typeTag
    val filter = filterFromSpec[T](spec)
    val values = RestrictAttributeToIds.run(attr, vertices).value
    values.filter { case (id, value) => filter.matches(value) }.keySet
  }

  def embedFilteredVertices(
    base: VertexSet, filters: Seq[FEVertexAttributeFilter], heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    embedFilteredVerticesFA(base, filters.map(_.toFilteredAttribute), heavy)
  }

  def embedFilteredVerticesFA(
    base: VertexSet, filters: Seq[FilteredAttribute[_]], heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    for (v <- filters) {
      assert(v.attribute.vertexSet == base, s"Filter mismatch: ${v.attribute} and $base")
    }
    intersectionEmbedding(base +: filters.map(applyFilter(_)), heavy)
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter])(
    implicit metaManager: MetaGraphManager): VertexSet = {
    embedFilteredVertices(filtered, moreFilters).srcVertexSet
  }

  private def applyFilter[T](
    fa: FilteredAttribute[T])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    import Scripting._
    val op = VertexAttributeFilter(fa.filter)
    return op(op.attr, fa.attribute).result.fvs
  }

  private def intersectionEmbedding(
    filteredVss: Seq[VertexSet], heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {

    val op = VertexSetIntersection(filteredVss.size, heavy)
    op(op.vss, filteredVss).result.firstEmbedding
  }

  def filterFromSpec[T: TypeTag](spec: String): Filter[T] = {
    if (spec.startsWith("!")) {
      NotFilter(filterFromSpec(spec.drop(1)))
    } else if (spec == "*") {
      MatchAllFilter()
    } else if (typeOf[T] =:= typeOf[String]) {
      val stringFilter = spec match {
        case regexRE(re) => RegexFilter(re)
        case csv => OneOf(csv.split(",", -1).map(_.trim).toSet)
      }
      stringFilter.asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[Long]) {
      OneOf(spec.split(",", -1).map(_.trim.toLong).toSet)
        .asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[(Double, Double)]) {
      def intervalToFilter(interval: String): AndFilter[Double] = {
        interval match {
          case intervalOpenOpenRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLT(b.toDouble))
          case intervalOpenCloseRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLE(b.toDouble))
          case intervalCloseOpenRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLT(b.toDouble))
          case intervalCloseCloseRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLE(b.toDouble))
        }
      }
      spec match {
        case geoRE(latInterval, lonInterval) =>
          PairFilter(intervalToFilter(latInterval), intervalToFilter(lonInterval))
            .asInstanceOf[Filter[T]]
        case filter => throw new AssertionError(s"Not a valid filter: $filter.")
      }
    } else if (typeOf[T] =:= typeOf[Double]) {
      val doubleFilter = spec match {
        case numberRE(num) => DoubleEQ(num.toDouble)
        case intervalOpenOpenRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLT(b.toDouble))
        case intervalOpenCloseRE(a, b) => AndFilter(DoubleGT(a.toDouble), DoubleLE(b.toDouble))
        case intervalCloseOpenRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLT(b.toDouble))
        case intervalCloseCloseRE(a, b) => AndFilter(DoubleGE(a.toDouble), DoubleLE(b.toDouble))
        case boundRE(comparator, valueString) =>
          val value = valueString.toDouble
          comparator match {
            case "=" => DoubleEQ(value)
            case "==" => DoubleEQ(value)
            case "<" => DoubleLT(value)
            case ">" => DoubleGT(value)
            case "<=" => DoubleLE(value)
            case ">=" => DoubleGE(value)
            case comparator => throw new AssertionError(s"Not a valid comparator: $comparator")
          }
        case filter => throw new AssertionError(s"Not a valid filter: $filter")
      }
      doubleFilter.asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[(ID, ID)]) {
      spec match {
        case "=" => PairEquals[ID]().asInstanceOf[Filter[T]]
        case filter =>
          throw new AssertionError(s"Not a valid filter: $filter (The only valid filter is '='.)")
      }
    } else if (typeOf[T] <:< typeOf[Vector[Any]]) {
      val elementTypeTag = TypeTagUtil.typeArgs(typeTag[T]).head
      spec match {
        case existsRE(elementSpec) =>
          Exists(filterFromSpec(elementSpec)(elementTypeTag)).asInstanceOf[Filter[T]]
        case forallRE(elementSpec) =>
          ForAll(filterFromSpec(elementSpec)(elementTypeTag)).asInstanceOf[Filter[T]]
        case filter => throw new AssertionError(s"Not a valid filter: $filter")
      }
    } else ???
  }

  private val number = "-?\\d*(?:\\.\\d*)?"
  private val numberWithSpaces = s"\\s*$number\\s*"
  private val numberPattern = s"\\s*($number)\\s*"
  private val numberRE = numberPattern.r
  private val intervalOpenOpenRE = s"\\s*\\($numberPattern,$numberPattern\\)\\s*".r
  private val intervalOpenCloseRE = s"\\s*\\($numberPattern,$numberPattern\\]\\s*".r
  private val intervalCloseOpenRE = s"\\s*\\[$numberPattern,$numberPattern\\)\\s*".r
  private val intervalCloseCloseRE = s"\\s*\\[$numberPattern,$numberPattern\\]\\s*".r
  private val intervalPattern = s"\\s*([\\(\\[]$numberWithSpaces,$numberWithSpaces[\\)\\]])\\s*"
  private val geoRE = s"\\s*\\($intervalPattern,$intervalPattern\\)\\s*".r
  private val comparatorPattern = "\\s*(<|>|==?|<=|>=)\\s*"
  private val boundRE = s"$comparatorPattern$numberPattern".r
  private val forallRE = "\\s*(?:forall|all|Ɐ)\\((.*)\\)\\s*".r
  private val existsRE = "\\s*(?:exists|any|some|∃)\\((.*)\\)\\s*".r
  private val regexRE = "\\s*regexp?\\((.*)\\)\\s*".r
}
