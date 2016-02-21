package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript

class JavaScriptTest extends FunSuite {
  test("test Boolean expressions") {
    assert(JavaScript("true").isTrue(Map[String, String]()))
    assert(JavaScript("1.0").isTrue(Map[String, String]()))
    assert(JavaScript("'a'").isTrue(Map[String, String]()))
    assert(JavaScript("").isTrue(Map[String, String]()))

    assert(!JavaScript("false").isTrue(Map[String, String]()))
    assert(!JavaScript("0.0").isTrue(Map[String, String]()))
    assert(!JavaScript("''").isTrue(Map[String, String]()))
    assert(!JavaScript("undefined").isTrue(Map[String, String]())) // undefined -> false
  }

  test("test String expressions") {
    assert(JavaScript("true").evaluateString(Map[String, String]()) == Some("true"))
    assert(JavaScript("1.0").evaluateString(Map[String, String]()) == Some("1"))
    assert(JavaScript("'a'").evaluateString(Map[String, String]()) == Some("a"))
    assert(JavaScript("undefined").evaluateString(Map[String, String]()) == None)
  }

  def checkDoubleError(expr: String, msg: String): Unit = {
    try {
      JavaScript(expr).evaluateDouble(Map[String, String]())
      fail
    } catch {
      case e: IllegalArgumentException => assert(e.getMessage == msg)
    }
  }

  test("test Double expressions") {
    assert(JavaScript("1.0").evaluateDouble(Map[String, String]()) == Some(1.0))
    assert(JavaScript("'1.0'").evaluateDouble(Map[String, String]()) == Some(1.0))
    assert(JavaScript("undefined").evaluateDouble(Map[String, String]()) == None)

    assert(JavaScript("1.0/0.0").evaluateDouble(Map[String, String]()).get.isInfinite)
    assert(JavaScript("0.0/0.0").evaluateDouble(Map[String, String]()).get.isNaN)

    checkDoubleError("'abc'", "JavaScript('abc') with values: {} did not return a valid number: abc")
    checkDoubleError("true", "JavaScript(true) with values: {} did not return a valid number: true")
  }

  test("test variable substitution") {
    assert(JavaScript("a").evaluateDouble(Map("a" -> 1.0)) == Some(1.0))
    assert(JavaScript("a").evaluateString(Map("a" -> 1.0)) == Some("1"))
    assert(JavaScript("a").isTrue(Map("a" -> "1.0")))
  }
}

