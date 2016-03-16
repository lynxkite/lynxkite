package com.lynxanalytics.biggraph

import org.scalatest.FunSuite

class JavaScriptTest extends FunSuite {
  val noArgs = Map[String, String]() // For nicer readability.

  test("test Boolean expressions") {
    assert(JavaScript("true").evaluator.evaluateBoolean(noArgs) == Some(true))
    assert(JavaScript("1.0").evaluator.evaluateBoolean(noArgs) == Some(true))
    assert(JavaScript("'a'").evaluator.evaluateBoolean(noArgs) == Some(true))

    assert(JavaScript("false").evaluator.evaluateBoolean(noArgs) == Some(false))
    assert(JavaScript("0.0").evaluator.evaluateBoolean(noArgs) == Some(false))
    assert(JavaScript("''").evaluator.evaluateBoolean(noArgs) == Some(false))

    assert(JavaScript("").evaluator.evaluateBoolean(noArgs) == None)
    assert(JavaScript("undefined").evaluator.evaluateBoolean(noArgs) == None)
  }

  test("test String expressions") {
    assert(JavaScript("true").evaluator.evaluateString(noArgs) == Some("true"))
    assert(JavaScript("1.0").evaluator.evaluateString(noArgs) == Some("1"))
    assert(JavaScript("'a'").evaluator.evaluateString(noArgs) == Some("a"))
    assert(JavaScript("undefined").evaluator.evaluateString(noArgs) == None)
  }

  test("test Double expressions") {
    assert(JavaScript("1.0").evaluator.evaluateDouble(noArgs) == Some(1.0))
    assert(JavaScript("'1.0'").evaluator.evaluateDouble(noArgs) == Some(1.0))
    assert(JavaScript("undefined").evaluator.evaluateDouble(noArgs) == None)

    assert(JavaScript("1.0/0.0").evaluator.evaluateDouble(noArgs).get.isInfinite)
    assert(JavaScript("0.0/0.0").evaluator.evaluateDouble(noArgs).get.isNaN)
    assert(JavaScript("Math.log(-1)").evaluator.evaluateDouble(noArgs).get.isNaN)

    assert(JavaScript("'abc'").evaluator.evaluateDouble(noArgs).get.isNaN)
    assert(JavaScript("false").evaluator.evaluateDouble(noArgs) == Some(0.0))
    assert(JavaScript("true").evaluator.evaluateDouble(noArgs) == Some(1.0))
  }

  test("test variable substitution") {
    assert(JavaScript("a").evaluator.evaluateDouble(Map("a" -> 1.0)) == Some(1.0))
    assert(JavaScript("a").evaluator.evaluateString(Map("a" -> 1.0)) == Some("1"))
    assert(JavaScript("a").evaluator.evaluateBoolean(Map("a" -> "1.0")) == Some(true))
  }
}

