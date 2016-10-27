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
    assert(FEFilters.filterFromSpec[Double]("123") == DoubleEQ(123.0))
    assert(FEFilters.filterFromSpec[Double]("123.5") == DoubleEQ(123.5))
  }
  test("half-bound range for double") {
    assert(FEFilters.filterFromSpec[Double]("<123") == DoubleLT(123.0))
    assert(FEFilters.filterFromSpec[Double]("<=123") == DoubleLE(123.0))
    assert(FEFilters.filterFromSpec[Double](">123") == DoubleGT(123.0))
    assert(FEFilters.filterFromSpec[Double](">=123") == DoubleGE(123.0))
    assert(FEFilters.filterFromSpec[Double]("==123") == DoubleEQ(123.0))
  }
  test("interval for double") {
    assert(FEFilters.filterFromSpec[Double]("[12,34]") == AndFilter(DoubleGE(12.0), DoubleLE(34.0)))
    assert(FEFilters.filterFromSpec[Double]("[12,34)") == AndFilter(DoubleGE(12.0), DoubleLT(34.0)))
    assert(FEFilters.filterFromSpec[Double]("(12,34]") == AndFilter(DoubleGT(12.0), DoubleLE(34.0)))
  }
  test("syntax error") {
    intercept[AssertionError] {
      FEFilters.filterFromSpec[Double]("asd")
    }
  }
  test("negation") {
    assert(FEFilters.filterFromSpec[Double]("!123") == NotFilter(DoubleEQ(123.0)))
  }
}
