package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveJSTest extends FunSuite with TestGraphOp {
  test("example graph: 'name.length * 10 + age'") {
    val expr = "name.length * 10 + age"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      Seq("age", "name"), Seq())
    val derived = op(
      op.attrs,
      VertexAttributeToJSValue.seq(g.age.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 60.3, 1 -> 48.2, 2 -> 80.3, 3 -> 122.0))
  }

  ignore("example graph: global") {
    val expr = "global$greeting"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr), Seq(),
      Seq("global$greeting"))
    // TODO
  }

  test("example graph: \"gender == 'Male' ? 'Mr ' + name : 'Ms ' + name\"") {
    val expr = "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      Seq("gender", "name"), Seq())
    val derived = op(
      op.attrs,
      VertexAttributeToJSValue.seq(g.gender.entity, g.name.entity)).result.attr
    assert(derived.rdd.collect.toSet == Set(
      0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob", 3 -> "Mr Isolated Joe"))
  }

  test("DeriveJS works with no input attributes (vertices)") {
    val expr = "1.0"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      Seq(), Seq())
    val derived = op(op.vs, g.vertices.entity)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0))
  }

  test("JS integers become Scala doubles") {
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(JavaScript("2"), Seq(), Seq())
    val derived = op(op.vs, g.vertices.entity)(op.attrs, Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> 2.0, 1 -> 2.0, 2 -> 2.0, 3 -> 2.0))
  }

  test("JS integers are accepted as Scala doubles") {
    DeriveJS.validateJS[Double](JavaScript("2"), Seq(), Seq())
  }

  test("DeriveJS works with no input attributes (edges)") {
    val expr = "'hallo'"
    val g = ExampleGraph()().result
    val op = DeriveJSString(
      JavaScript(expr),
      Seq(), Seq())
    val derived = op(op.vs, g.edges.idSet)(
      op.attrs,
      Seq()).result.attr
    assert(derived.rdd.collect.toSet == Set(0 -> "hallo", 1 -> "hallo", 2 -> "hallo", 3 -> "hallo"))
  }

}
