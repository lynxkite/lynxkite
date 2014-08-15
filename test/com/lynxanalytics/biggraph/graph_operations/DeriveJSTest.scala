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
    val op = DeriveJS(JavaScript(expr), numAttrNames = Seq("age"), strAttrNames = Seq("name"))
    val derived = op(op.numAttrs, Seq(g.age.entity))(op.strAttrs, Seq(g.name.entity)).result.attr
    assert(derived.rdd.collect.toSeq == Seq(0 -> "60.3", 1 -> "48.2", 2 -> "80.3", 3 -> "122.0"))
  }
}
