package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript

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
    assert(JavaScript("true").evaluateString(noArgs) == Some("true"))
    assert(JavaScript("1.0").evaluateString(noArgs) == Some("1"))
    assert(JavaScript("'a'").evaluateString(noArgs) == Some("a"))
    assert(JavaScript("undefined").evaluateString(noArgs) == None)
  }

  def checkDoubleError(expr: String, msg: String): Unit = {
    try {
      JavaScript(expr).evaluateDouble(noArgs)
      fail
    } catch {
      case e: java.lang.AssertionError => assert(e.getMessage == msg)
    }
  }

  test("test Double expressions") {
    assert(JavaScript("1.0").evaluateDouble(noArgs) == Some(1.0))
    assert(JavaScript("'1.0'").evaluateDouble(noArgs) == Some(1.0))
    assert(JavaScript("undefined").evaluateDouble(noArgs) == None)

    assert(JavaScript("1.0/0.0").evaluateDouble(noArgs).get.isInfinite)
    assert(JavaScript("0.0/0.0").evaluateDouble(noArgs).get.isNaN)
    assert(JavaScript("Math.log(-1)").evaluateDouble(noArgs).get.isNaN)

    checkDoubleError("'abc'", "JavaScript('abc') with values: {} did not return a valid number: abc")
    checkDoubleError("true", "JavaScript(true) with values: {} did not return a valid number: true")
  }

  test("test variable substitution") {
    assert(JavaScript("a").evaluateDouble(Map("a" -> 1.0)) == Some(1.0))
    assert(JavaScript("a").evaluateString(Map("a" -> 1.0)) == Some("1"))
    assert(JavaScript("a").isTrue(Map("a" -> "1.0")))
  }
}

