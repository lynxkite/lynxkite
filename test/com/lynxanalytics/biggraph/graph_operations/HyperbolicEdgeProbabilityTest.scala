package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.math
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HyperbolicEdgeProbabilityTest extends FunSuite with TestGraphOp {
  test("radial connections with high probability") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2, 3), 1 -> Seq(0),
      2 -> Seq(0), 3 -> Seq(0), 4 -> Seq()))().result
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
    val degreeOp = OutDegree()
    val degree = degreeOp(degreeOp.es, g.es).result.outDegree
    val clusterOp = ApproxClusteringCoefficient(8)
    val clus = clusterOp(clusterOp.vs, g.vs)(clusterOp.es, g.es).result.clustering
    val op = HyperbolicEdgeProbability()
    val radial = AddVertexAttribute.run(g.vs, radialMap)
    val angular = AddVertexAttribute.run(g.vs, angularMap)

    val out = op(op.vs, g.vs)(op.es, g.es)(op.radial, radial
    )(op.angular, angular)(op.degree, degree)(op.clustering, clus).result
    val probs = out.edgeProbability.rdd
    
    assert(probs.filter { case (id, prob) => prob < 0.8 }.isEmpty)
  }
  test("lateral connections with low probability") {
    val g = SmallTestGraph(Map(0 -> Seq(), 1 -> Seq(2, 3),
      2 -> Seq(1), 3 -> Seq(1, 4), 4 -> Seq(3)))().result
    val radialMap = Map[Int, Double](0 -> 0.0,
      1 -> 2 * math.log(6),
      2 -> 2 * math.log(7),
      3 -> 2 * math.log(8),
      4 -> 2 * math.log(9))
    val angularMap = Map[Int, Double](0 -> 0.0,
      1 -> (math.Pi * 0.5),
      2 -> (math.Pi * 1),
      3 -> (math.Pi * 1.5),
      4 -> 0)
    val degreeOp = OutDegree()
    val degree = degreeOp(degreeOp.es, g.es).result.outDegree
    val clusterOp = ApproxClusteringCoefficient(8)
    val clus = clusterOp(clusterOp.vs, g.vs)(clusterOp.es, g.es).result.clustering
    val op = HyperbolicEdgeProbability()
    val radial = AddVertexAttribute.run(g.vs, radialMap)
    val angular = AddVertexAttribute.run(g.vs, angularMap)

    val out = op(op.vs, g.vs)(op.es, g.es)(op.radial, radial
    )(op.angular, angular)(op.degree, degree)(op.clustering, clus).result
    val probs = out.edgeProbability.rdd

    assert(probs.filter { case (id, prob) => prob > 0.2 }.isEmpty)
  }
}
