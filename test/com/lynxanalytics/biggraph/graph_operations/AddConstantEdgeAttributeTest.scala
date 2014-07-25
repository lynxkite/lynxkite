package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import org.apache.spark.SparkContext.rddToPairRDDFunctions

class AddConstantEdgeAttributeTest extends FunSuite with TestGraphOp {
  test("triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0))).result
    val op = AddConstantDoubleEdgeAttribute(100.0)
    val out = op(op.edges, g.es).result

    // join edge bundle and weight data to make an output that is easy to read
    val res = g.es.rdd.join(out.attr.rdd).map {
      case (id, (edge, value)) =>
        (edge.src.toInt, edge.dst.toInt) -> value
    }.collect.toMap

    assert(res == Map((0l, 1l) -> 100.0, (1l, 2l) -> 100.0, (2l, 0l) -> 100.0))
  }
}
