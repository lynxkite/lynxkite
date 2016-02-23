package com.lynxanalytics.biggraph

import org.scalatest.FunSuite

class JavaScriptTest extends FunSuite {
  val noArgs = Map[String, String]() // For nicer readability.

  test("test Boolean expressions") {
    assert(JavaScript("true").isTrue(noArgs))
    assert(JavaScript("1.0").isTrue(noArgs))
    assert(JavaScript("'a'").isTrue(noArgs))
    assert(JavaScript("").isTrue(noArgs))

    assert(!JavaScript("false").isTrue(noArgs))
    assert(!JavaScript("0.0").isTrue(noArgs))
    assert(!JavaScript("''").isTrue(noArgs))
    assert(!JavaScript("undefined").isTrue(noArgs)) // undefined -> false
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
    assert(JavaScript("a").isTrue(Map("a" -> "1.0")))
  }
}

