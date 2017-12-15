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
    implicit
    manager: MetaGraphManager): Attribute[_] = {
    manager.attribute(attributeId.asUUID)
  }

  def toFilteredAttribute(
    implicit
    manager: MetaGraphManager): FilteredAttribute[_] = {
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
    implicit
    metaManager: MetaGraphManager): VertexSet = {
    filterFA(vertexSet, filters.map(_.toFilteredAttribute))
  }

  def filterFA(
    vertexSet: VertexSet, filters: Seq[FilteredAttribute[_]])(
    implicit
    metaManager: MetaGraphManager): VertexSet = {
    for (f <- filters) {
      assert(
        f.attribute.vertexSet == vertexSet,
        s"Filter $f does not match vertex set $vertexSet")
    }
    if (filters.isEmpty) return vertexSet
    intersectionEmbedding(filters.map(applyFilter(_))).srcVertexSet
  }

  def localFilter(
    vertices: Set[ID], filters: Seq[FEVertexAttributeFilter])(
    implicit
    metaManager: MetaGraphManager, dataManager: DataManager): Set[ID] = {
    filters.foldLeft(vertices) { (vs, filter) =>
      localFilter(vs, filter.attribute, filter.valueSpec)
    }
  }

  def localFilter[T](
    vertices: Set[ID], attr: Attribute[T], spec: String)(
    implicit
    metaManager: MetaGraphManager, dataManager: DataManager): Set[ID] = {
    implicit val tt = attr.typeTag
    val filter = filterFromSpec[T](spec)
    val values = RestrictAttributeToIds.run(attr, vertices).value
    values.filter { case (id, value) => filter.matches(value) }.keySet
  }

  def embedFilteredVertices(
    base: VertexSet, filters: Seq[FEVertexAttributeFilter], heavy: Boolean = false)(
    implicit
    metaManager: MetaGraphManager): EdgeBundle = {
    embedFilteredVerticesFA(base, filters.map(_.toFilteredAttribute), heavy)
  }

  def embedFilteredVerticesFA(
    base: VertexSet, filters: Seq[FilteredAttribute[_]], heavy: Boolean = false)(
    implicit
    metaManager: MetaGraphManager): EdgeBundle = {
    for (v <- filters) {
      assert(v.attribute.vertexSet == base, s"Filter mismatch: ${v.attribute} and $base")
    }
    intersectionEmbedding(base +: filters.map(applyFilter(_)), heavy)
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter])(
    implicit
    metaManager: MetaGraphManager): VertexSet = {
    embedFilteredVertices(filtered, moreFilters).srcVertexSet
  }

  private def applyFilter[T](
    fa: FilteredAttribute[T])(
    implicit
    metaManager: MetaGraphManager): VertexSet = {
    import Scripting._
    val op = VertexAttributeFilter(fa.filter)
    return op(op.attr, fa.attribute).result.fvs
  }

  private def intersectionEmbedding(
    filteredVss: Seq[VertexSet], heavy: Boolean = false)(
    implicit
    metaManager: MetaGraphManager): EdgeBundle = {

    val op = VertexSetIntersection(filteredVss.size, heavy)
    op(op.vss, filteredVss).result.firstEmbedding
  }

  import fastparse.all._
  private val quote = '"'
  private val backslash = '\\'

  private val quoteStr = s"${quote}"
  private val backslashStr = s"${backslash}"
  private val charsNotInSimpleString: String = s"${quote},()[]"

  private val notEscaped: Parser[Char] = P(CharPred(c => c != backslash && c != quote))
    .!.map(x => x(0)) // Make it a Char
  private val escapeSeq: Parser[Char] = P((backslashStr ~ quoteStr) | (backslashStr ~ backslashStr))
    .!.map(x => x(1)) // Strip the backslash from the front and make it a Char
  private val escapedString: Parser[String] = P(quoteStr ~ (notEscaped | escapeSeq).rep() ~ quoteStr)
    .map(x => x.mkString("")) // Assemble the Chars into a String
  private val simpleString: Parser[String] = P(CharPred(c => !charsNotInSimpleString.contains(c)).rep(1)).!

  private val ws = P(" ".rep())
  private val token: Parser[String] = P(ws ~ (escapedString | simpleString) ~ ws)

  private def parseSpec[T: TypeTag](
    tag: Type,
    spec: String,
    fromStringConverter: String => T): Filter[T] = {

    val interval = {
      val openOpen = P("(" ~ token ~ "," ~ token ~ ")").map {
        x => AndFilter(GT(fromStringConverter(x._1)), LT(fromStringConverter(x._2)))
      }
      val openClose = P("(" ~ token ~ "," ~ token ~ "]").map {
        x => AndFilter(GT(fromStringConverter(x._1)), LE(fromStringConverter(x._2)))
      }
      val closeOpen = P("[" ~ token ~ "," ~ token ~ ")").map {
        x => AndFilter(GE(fromStringConverter(x._1)), LT(fromStringConverter(x._2)))
      }
      val closeClose = P("[" ~ token ~ "," ~ token ~ "]").map {
        x => AndFilter(GE(fromStringConverter(x._1)), LE(fromStringConverter(x._2)))
      }
      P(openOpen | openClose | closeOpen | closeClose)
    }

    val commaSeparatedList = {
      P(token.rep(sep = ",", min = 1)).map {
        x =>
          if (x.size == 1) EQ(fromStringConverter(x.head))
          else OneOf(x.map(fromStringConverter).toSet).asInstanceOf[Filter[T]]
      }
    }
    val comparison = {
      val eq = P(("==" | "=") ~ token.!).map(x => EQ(fromStringConverter(x)))
      val lt = P("<" ~ !"=" ~ token.!).map(x => LT(fromStringConverter(x)))
      val le = P("<=" ~ token.!).map(x => LE(fromStringConverter(x)))
      val gt = P(">" ~ !"=" ~ token.!).map(x => GT(fromStringConverter(x)))
      val ge = P(">=" ~ token.!).map(x => GE(fromStringConverter(x)))

      P(eq | lt | le | gt | ge)
    }

    val geo = P(ws ~ interval ~ ws ~ "," ~ ws ~ interval ~ ws).map {
      x =>
        assert(
          tag =:= typeOf[(Double, Double)],
          s"geo filter doesn't make sense for $tag ")
        PairFilter(x._1, x._2).asInstanceOf[Filter[T]]
    }

    val regex = P(("regex(" | "regexp(") ~ token ~ ")").map {
      x =>
        assert(
          tag =:= typeOf[String],
          s"regex filter doesn't make sense for $tag ")
        RegexFilter(x).asInstanceOf[Filter[T]]
    }

    val expr = P(Start ~ (geo | comparison | regex | commaSeparatedList | interval) ~ End)
    import fastparse.core.Parsed
    val Parsed.Success(filter, _) = expr.parse(spec)
    filter
  }

  def filterFromSpec[T: TypeTag](spec: String): Filter[T] = {
    if (spec.startsWith("!")) {
      NotFilter(filterFromSpec(spec.drop(1)))
    } else if (spec == "*") {
      MatchAllFilter()
    } else if (typeOf[T] =:= typeOf[String]) {
      parseSpec(typeOf[T], spec, _.toString)
    } else if (typeOf[T] =:= typeOf[Long]) {
      parseSpec(typeOf[T], spec, _.toLong)
    } else if (typeOf[T] =:= typeOf[Double]) {
      parseSpec(typeOf[T], spec, _.toDouble)
    } else if (typeOf[T] =:= typeOf[(Double, Double)]) {
      parseSpec(typeOf[T], spec, _.toDouble)
    } else if (typeOf[T] <:< typeOf[Vector[Any]]) {
      val elementTypeTag = TypeTagUtil.typeArgs(typeTag[T]).head
      val forall =
        P(("forall" | "all" | "Ɐ") ~ ws ~ "(" ~ token.! ~ ")").map(
          x => ForAll(filterFromSpec(x)(elementTypeTag)).asInstanceOf[Filter[T]])
      val exists =
        P(("exists" | "some" | "any" | "∃") ~ ws ~ "(" ~ token.! ~ ")").map(
          x => Exists(filterFromSpec(x)(elementTypeTag)).asInstanceOf[Filter[T]])
      val expr = P(Start ~ (forall | exists) ~ End)
      import fastparse.core.Parsed
      val Parsed.Success(filter, _) = expr.parse(spec)
      filter
    } else if (typeOf[T] =:= typeOf[(ID, ID)]) {
      spec match {
        case "=" => PairEquals[ID]().asInstanceOf[Filter[T]]
        case filter =>
          throw new AssertionError(s"Not a valid filter: $filter (The only valid filter is '='.)")
      }
    } else ???
  }
}
