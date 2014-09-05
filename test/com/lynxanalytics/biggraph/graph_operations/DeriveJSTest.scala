package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveJSTest extends FunSuite with TestGraphOp {
  test("example graph: 'name.length * 10 + age'") {
    val expr = "name.length * 10 + age"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      numAttrNames = Seq("age"),
      strAttrNames = Seq("name"),
      vecAttrNames = Seq())
    val derived = op(op.numAttrs, Seq(g.age.entity))(op.strAttrs, Seq(g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 60.3, 1 -> 48.2, 2 -> 80.3, 3 -> 122.0))
  }

  test("example graph: \"gender == 'Male' ? 'Mr ' + name : 'Ms ' + name\"") {
    val expr = "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      numAttrNames = Seq(),
      strAttrNames = Seq("gender", "name"),
      vecAttrNames = Seq())
    val derived = op(op.numAttrs, Seq())(op.strAttrs, Seq(g.gender.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob", 3 -> "Mr Isolated Joe"))
  }
}
