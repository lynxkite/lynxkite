package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.math
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HyperMapTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = HyperMap(1.5, 0.6, 0.45, 1337)
    val out = op(op.vs, g.vertices)(op.es, g.edges).result
    val angulars = out.angular.rdd.collect.map { case (id, ang) => ang }
    // Isolated Joe really is isolated
    assert(math.abs(angulars(3) - angulars(2)) > 1)
    // Radial coordinates are correct
    assert(out.radial.rdd.collect.toMap == Map(0 -> 2 * math.log(1),
      1 -> 2 * math.log(2),
      2 -> 2 * math.log(3),
      3 -> 2 * math.log(4)))
  }
}
