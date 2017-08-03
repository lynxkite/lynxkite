package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.math
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HyperbolicPredictionTest extends FunSuite with TestGraphOp {
  test("small example graph") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0),
      2 -> Seq(0), 3 -> Seq(), 4 -> Seq()))().result
    val radialMap = Map[Int, Double](0 -> 0.0,
      1 -> 2 * math.log(2),
      2 -> 2 * math.log(3),
      3 -> 2 * math.log(4),
      4 -> 2 * math.log(5))
    val angularMap = Map[Int, Double](0 -> 0.0,
      1 -> 1.0,
      2 -> (math.Pi * 2 - 1.0),
      3 -> 0.5,
      4 -> math.Pi)
    val op = HyperbolicPrediction(8, 0.6)
    val radial = AddVertexAttribute.run(g.vs, radialMap)
    val angular = AddVertexAttribute.run(g.vs, angularMap)
    val out = op(op.vs, g.vs)(op.es, g.es)(op.radial, radial
    )(op.angular, angular).result.predictedEdges
    val resultEdges = out.rdd.collect.map { case (id, edge) => edge }
    assert(!resultEdges.filter { e => e.src == 1 && e.dst == 2 }.isEmpty)
    assert(!resultEdges.filter { e => e.src == 3 && e.dst == 0 }.isEmpty)
    assert(!resultEdges.filter { e => e.src == 3 && e.dst == 1 }.isEmpty)
  }
}
