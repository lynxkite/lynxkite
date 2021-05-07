package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations.DoubleAttributeToLong._

class SplitVerticesTest extends AnyFunSuite with TestGraphOp {

  test("example graph") {
    val g = ExampleGraph()().result
    val op = SplitVertices()
    val res = op(op.attr, g.weight).result
    assert(res.newVertices.rdd.count() == 10)
    assert(res.indexAttr.rdd.values.collect.toSeq.sorted ==
      Seq[Long](
        0, 0, 0, 0, // Everybody has index 0
        1, 1, 1,
        2, 2,
        3 // But only weight 4.0 has index 3
      ))
    assert(res.belongsTo.rdd.values.collect.toSeq.map { e => e.dst }.sorted ==
      Seq[Long](
        0, // id 0 (weight 1.0) has 1 copy
        1, 1,
        2, 2, 2,
        3, 3, 3, 3 // id 3 (weight 4.0) has 4 copies
      ))
  }

  test("Zero drops vertices - one hundred lonely guys") {
    val expr = "if (name == \"Isolated Joe\") 100.0 else 0.0"
    val g = ExampleGraph()().result
    val derived = DeriveScala.derive[Double](
      expr,
      Seq("name" -> g.name.entity))
    val splitOp = SplitVertices()
    val res = splitOp(splitOp.attr, derived).result
    assert(res.newVertices.rdd.count() == 100)
    assert(res.indexAttr.rdd.values.collect.toSeq.sorted ==
      (0 to 99))
    assert(res.belongsTo.rdd.values.collect.toSeq.map { e => e.dst }.toSet ==
      Set(3)) // Joe
  }

}
