// Parses the filters set on the UI and creates Filters and FilteredAttributes.
package com.lynxanalytics.lynxkite.controllers

import scala.reflect.runtime.universe._
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_operations.{Filter, _}

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
      vertexSet: VertexSet,
      filters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    filterFA(vertexSet, filters.map(_.toFilteredAttribute))
  }

  def filterFA(
      vertexSet: VertexSet,
      filters: Seq[FilteredAttribute[_]])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    for (f <- filters) {
      assert(
        f.attribute.vertexSet == vertexSet,
        s"Filter $f does not match vertex set $vertexSet")
    }
    if (filters.isEmpty) vertexSet
    else if (filters.size == 1) applyFilter(filters.head)
    else intersectionEmbedding(filters.map(applyFilter(_))).srcVertexSet
  }

  def localFilter(
      vertices: Set[ID],
      filters: Seq[FEVertexAttributeFilter])(
      implicit
      metaManager: MetaGraphManager,
      dataManager: DataManager): Set[ID] = {
    filters.foldLeft(vertices) { (vs, filter) =>
      localFilter(vs, filter.attribute, filter.valueSpec)
    }
  }

  def localFilter[T](
      vertices: Set[ID],
      attr: Attribute[T],
      spec: String)(
      implicit
      metaManager: MetaGraphManager,
      dataManager: DataManager): Set[ID] = {
    implicit val tt = attr.typeTag
    val filter = filterFromSpec[T](spec)
    val values = RestrictAttributeToIds.run(attr, vertices).value
    values.filter { case (id, value) => filter.matches(value) }.keySet
  }

  def embedFilteredVertices(
      base: VertexSet,
      filters: Seq[FEVertexAttributeFilter],
      heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    embedFilteredVerticesFA(base, filters.map(_.toFilteredAttribute), heavy)
  }

  def embedFilteredVerticesFA(
      base: VertexSet,
      filters: Seq[FilteredAttribute[_]],
      heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    for (v <- filters) {
      assert(v.attribute.vertexSet == base, s"Filter mismatch: ${v.attribute} and $base")
    }
    if (filters.size == 1) filterEmbedding(filters.head)
    else intersectionEmbedding(base +: filters.map(applyFilter(_)), heavy)
  }

  def filterMore(filtered: VertexSet, moreFilters: Seq[FEVertexAttributeFilter])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    embedFilteredVertices(filtered, moreFilters).srcVertexSet
  }

  private def filterEmbedding[T](
      fa: FilteredAttribute[T])(
      implicit metaManager: MetaGraphManager): EdgeBundle = {
    import Scripting._
    val op = VertexAttributeFilter(fa.filter)
    return op(op.attr, fa.attribute).result.identity
  }

  private def applyFilter[T](
      fa: FilteredAttribute[T])(
      implicit metaManager: MetaGraphManager): VertexSet = {
    import Scripting._
    val op = VertexAttributeFilter(fa.filter)
    return op(op.attr, fa.attribute).result.fvs
  }

  private def intersectionEmbedding(
      filteredVss: Seq[VertexSet],
      heavy: Boolean = false)(
      implicit metaManager: MetaGraphManager): EdgeBundle = {

    val op = VertexSetIntersection(filteredVss.size, heavy)
    op(op.vss, filteredVss).result.firstEmbedding
  }

  import fastparse.all._

  class TokenParser {
    val quote = '"'
    val backslash = '\\'

    val quoteStr = s"${quote}"
    val backslashStr = s"${backslash}"
    val charsNotInSimpleString: String = s"${quote},()[]"

    val notEscaped: Parser[Char] = P(CharPred(c => c != backslash && c != quote))
      .!.map(x => x(0)) // Make it a Char
    val escapeSeq: Parser[Char] = P((backslashStr ~ quoteStr) | (backslashStr ~ backslashStr))
      .!.map(x => x(1)) // Strip the backslash from the front and make it a Char
    val escapedString: Parser[String] = P(quoteStr ~ (notEscaped | escapeSeq).rep() ~ quoteStr)
      .map(x => x.mkString("")) // Assemble the Chars into a String
    val simpleString: Parser[String] = P(CharPred(c => !charsNotInSimpleString.contains(c)).rep(1)).!

    val ws = P(" ".rep())
    val notWs = P(CharPred(c => c != ' ').rep())
    val token: Parser[String] = P(ws ~ (escapedString | simpleString) ~ ws)
  }

  abstract class BaseTypedParser[T: TypeTag](fromStringConverter: Option[String => T]) extends TokenParser {
    implicit val st = SerializableType(typeTag[T])

    protected lazy val fromString = fromStringConverter.get
    val interval = {
      val openOpen = P("(" ~ token ~ "," ~ token ~ ")").map {
        x => AndFilter(GT(fromString(x._1)), LT(fromString(x._2)))
      }
      val openClose = P("(" ~ token ~ "," ~ token ~ "]").map {
        x => AndFilter(GT(fromString(x._1)), LE(fromString(x._2)))
      }
      val closeOpen = P("[" ~ token ~ "," ~ token ~ ")").map {
        x => AndFilter(GE(fromString(x._1)), LT(fromString(x._2)))
      }
      val closeClose = P("[" ~ token ~ "," ~ token ~ "]").map {
        x => AndFilter(GE(fromString(x._1)), LE(fromString(x._2)))
      }
      P(openOpen | openClose | closeOpen | closeClose)
    }

    val commaSeparatedList = {
      P(token.rep(sep = ",", min = 1)).map {
        x =>
          if (x.size == 1) EQ(fromString(x.head))
          else OneOf(x.map(fromString).toSet).asInstanceOf[Filter[T]]
      }
    }

    val comparison = {
      val eq = P(("==" | "=") ~ token.!).map(x => EQ(fromString(x)))
      val lt = P("<" ~ !"=" ~ token.!).map(x => LT(fromString(x)))
      val le = P("<=" ~ token.!).map(x => LE(fromString(x)))
      val gt = P(">" ~ !"=" ~ token.!).map(x => GT(fromString(x)))
      val ge = P(">=" ~ token.!).map(x => GE(fromString(x)))

      P(eq | lt | le | gt | ge)
    }

    val filter: P[Filter[T]]

    def parse(spec: String) = {
      import fastparse.core.Parsed
      val notFilter = P(Start ~ "!" ~ notWs.! ~ End).map { x =>
        NotFilter(filterFromSpec(x))
      }
      val allFilter = P(Start ~ "*" ~ End).map(_ => MatchAllFilter())
      val expr = P(notFilter | allFilter | filter)
      val Parsed.Success(result, _) = expr.parse(spec)
      result
    }
  }

  import SerializableType._

  object StringParser extends BaseTypedParser[String](Some(_.toString)) {
    val regex = P(("regex(" | "regexp(") ~ token ~ ")").map {
      x =>
        RegexFilter(x).asInstanceOf[Filter[String]]
    }
    val filter = P(Start ~ (comparison | regex | commaSeparatedList | interval) ~ End)
  }

  object LongParser extends BaseTypedParser[Long](Some(_.toLong)) {
    val filter = P(Start ~ (comparison | commaSeparatedList | interval) ~ End)
  }

  object DoubleParser extends BaseTypedParser[Double](Some(_.toDouble)) {
    val filter = P(Start ~ (comparison | commaSeparatedList | interval) ~ End)
  }

  class VectorParser extends TokenParser {
    def forall[T: TypeTag] =
      P(("forall" | "all" | "Ɐ") ~ ws ~ "(" ~ token.! ~ ")").map(x =>
        ForAll(filterFromSpec(x)(typeTag[T])).asInstanceOf[Filter[T]])
    def exists[T: TypeTag] =
      P(("exists" | "some" | "any" | "∃") ~ ws ~ "(" ~ token.! ~ ")").map(x =>
        Exists(filterFromSpec(x)(typeTag[T])).asInstanceOf[Filter[T]])
    def parse[T: TypeTag](spec: String): Filter[T] = {
      val outerNotFilter = P("!" ~ notWs.!).map { x =>
        NotFilter(filterFromSpec(x)(typeTag[Vector[T]]).asInstanceOf[Filter[T]])
      }
      val matchAllFilter = P("*").map(_ => MatchAllFilter().asInstanceOf[Filter[T]])
      val vec = P(forall | exists)
      val intervals = P(DoubleParser.interval ~ ws ~ "," ~ ws ~ DoubleParser.interval).map {
        x => VectorFilter(x._1, x._2).asInstanceOf[Filter[T]]
      }
      val expr = P(Start ~ ws ~ (outerNotFilter | matchAllFilter | vec | intervals) ~ ws ~ End)
      import fastparse.core.Parsed
      val Parsed.Success(filter, _) = expr.parse(spec)
      filter
    }
  }

  object IdIdParser extends BaseTypedParser[(ID, ID)](None) {
    val filter = P(Start ~ "=" ~ End).!.map(_ => PairEquals[ID]())
  }

  def filterFromSpec[T: TypeTag](spec: String): Filter[T] = {
    if (typeOf[T] =:= typeOf[String]) {
      StringParser.parse(spec).asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[Long]) {
      LongParser.parse(spec).asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[Double]) {
      DoubleParser.parse(spec).asInstanceOf[Filter[T]]
    } else if (typeOf[T] <:< typeOf[Vector[Any]]) {
      val elementTypeTag = TypeTagUtil.typeArgs(typeTag[T]).head
      new VectorParser().parse(spec)(elementTypeTag).asInstanceOf[Filter[T]]
    } else if (typeOf[T] =:= typeOf[(ID, ID)]) {
      IdIdParser.parse(spec).asInstanceOf[Filter[T]]
    } else ???
  }
}
