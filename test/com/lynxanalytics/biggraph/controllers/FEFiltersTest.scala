package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FEFiltersTest extends FunSuite with TestGraphOp {

  test("specific value for string") {
    assert(FEFilters.filterFromSpec[String]("asd") == OneOf(Set("asd")))
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

  test("position test") {
    assert(FEFilters.filterFromSpec[(Double, Double)]("(12,34),[1,1.5)").matches((13, 1)))
    assert(!FEFilters.filterFromSpec[(Double, Double)]("(12,34),[1,1.5)").matches((12, 1)))
    assert(FEFilters.filterFromSpec[(Double, Double)]("[12,34),[1,1.5)").matches((12, 1)))
    assert(FEFilters.filterFromSpec[(Double, Double)]("(12,34],[1,1.5)").matches((34, 1)))
    assert(!FEFilters.filterFromSpec[(Double, Double)]("(12,34),[1,1.5)").matches((34, 1)))
    assert(!FEFilters.filterFromSpec[(Double, Double)]("(12,34),(1,1.5)").matches((12, 1)))
    assert(!FEFilters.filterFromSpec[(Double, Double)]("(12,34),(1,1.5)").matches((13, 1)))
    assert(!FEFilters.filterFromSpec[(Double, Double)]("(12,34),(1,1.5)").matches((13, 1.5)))
    assert(FEFilters.filterFromSpec[(Double, Double)]("(12,34),(1,1.5]").matches((13, 1.5)))
    // Make sure spaces are okay.
    assert(
      FEFilters.filterFromSpec[(Double, Double)](" ( 12 , 34 ) , ( 1 , 1.5 ] ").matches((13, 1.5)))
  }
  test("syntax error") {
    intercept[AssertionError] {
      FEFilters.filterFromSpec[Double]("asd")
    }
  }
  test("negation") {
    assert(FEFilters.filterFromSpec[Double]("!123") == NotFilter(EQ(123.0)))
    assert(FEFilters.filterFromSpec[Double]("!!123") == NotFilter(NotFilter(EQ(123.0))))
    assert(FEFilters.filterFromSpec[Double]("!!!123") == NotFilter(NotFilter(NotFilter(EQ(123.0)))))
  }
}
