package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.{ JavaScript, graph_operations }
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitVerticesTest extends FunSuite with TestGraphOp {

  def convAttr(attribute: Attribute[Double]): Attribute[Long] = {
    val convOp = graph_operations.DoubleAttributeToLong
    val attr = convOp.run(attribute)
    attr
  }

  test("example graph") {
    val g = ExampleGraph()().result

    val longAttr = convAttr(g.weight)
    val op = SplitVertices()
    val res = op(op.attr, longAttr).result
    assert(res.newVertices.rdd.count() == 10)
    assert(res.indexAttr.rdd.values.collect.toSeq.sorted ==
      Seq[Long](
        0, 0, 0, 0, // Everybody has index 0
        1, 1, 1,
        2, 2,
        3 // But only weight 4.0 has index 3
      ))
    assert(res.belongsTo.rdd.values.collect.toSeq.map { e => e.src }.sorted ==
      Seq[Long](
        0, // id 0 (weight 1.0) has 1 copy
        1, 1,
        2, 2, 2,
        3, 3, 3, 3 // id 3 (weight 4.0) has 4 copies 
      ))
  }

  test("Zero drops vertices - one hundred lonely guys") {
    val expr = "name == 'Isolated Joe' ? 100.0 : 0.0"
    val g = ExampleGraph()().result
    val op = DeriveJSDouble(
      JavaScript(expr),
      Seq("name"))
    val derived = op(
      op.attrs,
      VertexAttributeToJSValue.seq(g.name.entity)).result.attr
    val splitOp = SplitVertices()
    val res = splitOp(splitOp.attr, convAttr(derived)).result
    assert(res.newVertices.rdd.count() == 100)
    assert(res.indexAttr.rdd.values.collect.toSeq.sorted ==
      (0 to 99))
    assert(res.belongsTo.rdd.values.collect.toSeq.map { e => e.src }.toSet ==
      Set(3)) // Joe
  }

}
