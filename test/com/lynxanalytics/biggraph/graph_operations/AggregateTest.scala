package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateTest extends FunSuite with TestGraphOp {
  test("example graph components") {
    val example = ExampleGraph()().result
    val components = {
      val op = ConnectedComponents()
      op(op.es, example.edges).result
    }
    val count = {
      val op = AggregateByCount[Double]()
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(count.attr.rdd.collect.toSet == Set(0 -> 2, 2 -> 1, 3 -> 1))
    val sum = {
      val op = AggregateBySum()
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(sum.attr.rdd.collect.toSet == Set(0 -> 38.5, 2 -> 50.3, 3 -> 2.0))
    val average = {
      val op = AggregateByAverage()
      op(op.connection, components.belongsTo)(op.attr, example.age).result
    }
    assert(average.attr.rdd.collect.toSet == Set(0 -> 19.25, 2 -> 50.3, 3 -> 2.0))
  }
}
