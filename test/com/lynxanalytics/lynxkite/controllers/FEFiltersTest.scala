package com.lynxanalytics.lynxkite.controllers

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_operations._
import com.lynxanalytics.lynxkite.graph_util._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import play.api.libs.json

class FEFiltersTest extends AnyFunSuite with TestGraphOp {
  implicit val d = SerializableType.double
  implicit val i = SerializableType.int
  implicit val s = SerializableType.string
  implicit val l = SerializableType.long
  test("specific value for string") {
    assert(FEFilters.filterFromSpec[String]("asd") == EQ("asd"))
  }
  test("multiple values for string") {
    assert(FEFilters.filterFromSpec[String]("asd,qwe") == OneOf(Set("asd", "qwe")))
  }
  test("specific value for double") {
    assert(FEFilters.filterFromSpec[Double]("123") == EQ(123.0))
    assert(FEFilters.filterFromSpec[Double]("123.5") == EQ(123.5))
  }
  test("equality works for string") {
    assert(FEFilters.filterFromSpec[String]("==hehe") == EQ("hehe"))
  }
  test("less works for string") {
    assert(FEFilters.filterFromSpec[String]("<hehe") == LT[String]("hehe"))
  }

  test("half-bound range for double") {
    assert(FEFilters.filterFromSpec[Double]("<123") == LT(123.0))
    assert(FEFilters.filterFromSpec[Double]("<=123") == LE(123.0))
    assert(FEFilters.filterFromSpec[Double](">123") == GT(123.0))
    assert(FEFilters.filterFromSpec[Double](">=123") == GE(123.0))
    assert(FEFilters.filterFromSpec[Double]("==123") == EQ(123.0))
  }

  test("half-bound range for long") {
    assert(FEFilters.filterFromSpec[Long]("<123") == LT(123L))
    assert(FEFilters.filterFromSpec[Long]("<=123") == LE(123L))
    assert(FEFilters.filterFromSpec[Long](">123") == GT(123L))
    assert(FEFilters.filterFromSpec[Long](">=123") == GE(123L))
    assert(FEFilters.filterFromSpec[Long]("==123") == EQ(123L))
  }

  test("interval for double") {
    assert(FEFilters.filterFromSpec[Double]("[12,34]") == AndFilter(GE(12.0), LE(34.0)))
    assert(FEFilters.filterFromSpec[Double]("[12,34)") == AndFilter(GE(12.0), LT(34.0)))
    assert(FEFilters.filterFromSpec[Double]("(12,34]") == AndFilter(GT(12.0), LE(34.0)))
  }

  test("interval for string") {
    val filter = FEFilters.filterFromSpec[String]("[alma,narancs]")
    assert(filter.matches("alma"))
    assert(filter.matches("barack"))
    assert(filter.matches("narancs"))
    assert(!filter.matches("szilva"))
    val filter2 = FEFilters.filterFromSpec[String]("(alma,narancs)")
    assert(!filter2.matches("alma"))
    assert(filter2.matches("barack"))
    assert(!filter2.matches("narancs"))
  }

  test("Regex type error") {
    intercept[NumberFormatException] {
      FEFilters.filterFromSpec[Double]("regexp(123.0)")
    }
  }

  test("Geofilter syntax error") {
    intercept[MatchError] {
      FEFilters.filterFromSpec[String]("(alma,narancs), (alma,narancs)")
    }
  }

  test("Regex gets parsed") {
    val complexRegex = "\"(a) | (b)\""
    val expectedAfterParse = "(a) | (b)"
    assert(FEFilters.filterFromSpec[String](s"regexp($complexRegex)")
      == RegexFilter(expectedAfterParse))

    val simpleRegex = "abc.*"
    assert(FEFilters.filterFromSpec[String](s"regexp($simpleRegex)")
      == RegexFilter(simpleRegex))
  }

  test("We're backward compatible") {
    def readOld(classpath: String, varName: String) = {
      val oldJson = json.Json.obj(
        "class" -> ("com.lynxanalytics.lynxkite.graph_operations." + classpath),
        "data" -> json.Json.obj(varName -> 0.0))
      com.lynxanalytics.lynxkite.graph_api.TypedJson.read[Any](oldJson)
    }

    assert(readOld("DoubleLT", "bound") == LT[Double](0.0))
    assert(readOld("DoubleLE", "bound") == LE[Double](0.0))
    assert(readOld("DoubleGT", "bound") == GT[Double](0.0))
    assert(readOld("DoubleGE", "bound") == GE[Double](0.0))
    assert(readOld("DoubleEQ", "exact") == EQ[Double](0.0))
  }

  test("position test") {
    assert(FEFilters.filterFromSpec[Vector[Double]]("(12,34),[1,1.5)").matches(Vector(13, 1)))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("(12,34),[1,1.5)").matches(Vector(12, 1)))
    assert(FEFilters.filterFromSpec[Vector[Double]]("[12,34),[1,1.5)").matches(Vector(12, 1)))
    assert(FEFilters.filterFromSpec[Vector[Double]]("(12,34],[1,1.5)").matches(Vector(34, 1)))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("(12,34),[1,1.5)").matches(Vector(34, 1)))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("(12,34),(1,1.5)").matches(Vector(12, 1)))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("(12,34),(1,1.5)").matches(Vector(13, 1)))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("(12,34),(1,1.5)").matches(Vector(13, 1.5)))
    assert(FEFilters.filterFromSpec[Vector[Double]]("(12,34),(1,1.5]").matches(Vector(13, 1.5)))
    // Make sure spaces are okay.
    assert(
      FEFilters.filterFromSpec[Vector[Double]](" ( 12 , 34 ) , ( 1 , 1.5 ] ").matches(Vector(13, 1.5)))
  }

  test("syntax error") {
    intercept[scala.MatchError] {
      FEFilters.filterFromSpec[Double]("(asd")
    }
  }
  test("negation") {
    assert(FEFilters.filterFromSpec[Double]("!123") == NotFilter(EQ(123.0)))
    assert(FEFilters.filterFromSpec[Double]("!!123") == NotFilter(NotFilter(EQ(123.0))))
    assert(FEFilters.filterFromSpec[Double]("!!!123") == NotFilter(NotFilter(NotFilter(EQ(123.0)))))
    assert(FEFilters.filterFromSpec[Double]("!!!*") == NotFilter(NotFilter(NotFilter(MatchAllFilter()))))
    assert(FEFilters.filterFromSpec[String]("!\"well\"") == NotFilter(EQ("well")))
    assert(FEFilters.filterFromSpec[Double]("!1.0,2.0") == NotFilter(OneOf(Set(1.0, 2.0))))
  }

  test("vectors work") {
    val v = List(1.0, 2.0, 3.0).toVector
    assert(FEFilters.filterFromSpec[Vector[Double]]("exists(==2.0)").matches(v))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("exists(==4.0)").matches(v))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("forall (==2.0)").matches(v))
    assert(FEFilters.filterFromSpec[Vector[Double]]("forall (<10.0)").matches(v))

  }
  test("more complex vector tests") {
    val v = List(1.0, 2.0, 3.0).toVector
    assert(FEFilters.filterFromSpec[Vector[Double]]("exists(!>1.0)").matches(v))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("exists(!>=1.0)").matches(v))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("!exists(>1.0)").matches(v))
    assert(!FEFilters.filterFromSpec[Vector[Double]]("!exists(!>1.0)").matches(v))
    assert(FEFilters.filterFromSpec[Vector[String]]("!!all(!!*)") ==
      NotFilter(NotFilter(ForAll(NotFilter(NotFilter(MatchAllFilter()))))))
  }
  test("Escaped strings") { // "ab\\cd\"ef" == ab\cd"ef
    val quote = '"'
    val backslash = '\\'

    val str = s"${quote}ab${backslash}${backslash}cd${backslash}${quote}ef${quote}"
    val expected = s"ab${backslash}cd${quote}ef"
    assert(FEFilters.filterFromSpec[String](str) == EQ[String](expected))
  }
}
