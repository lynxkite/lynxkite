package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.math
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HyperMapTest extends FunSuite with TestGraphOp {
  test("small example graph") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2, 3), 1 -> Seq(0, 2),
      2 -> Seq(0, 1), 3 -> Seq(0), 4 -> Seq()))().result
    val degreeOp = OutDegree()
    val degree = degreeOp(degreeOp.es, g.es).result.outDegree
    val clusterOp = ApproxClusteringCoefficient(8)
    val clus = clusterOp(clusterOp.vs, g.vs)(clusterOp.es, g.es).result.clustering
    val op = HyperMap(42)
    val out = op(op.vs, g.vs)(op.es, g.es)(
      op.degree, degree)(op.clustering, clus).result
    val angulars = out.angular.rdd.collect.toMap
    // Isolated Joe really is isolated
    assert(math.abs(angulars.get(4).get - angulars.get(1).get) > 1 &&
      math.abs(2 * math.Pi - math.abs(angulars.get(4).get
        - angulars.get(1).get)) > 1)
    // Radial coordinates are correct
    assert(out.radial.rdd.collect.toMap == Map(0 -> 2 * math.log(1),
      1 -> 2 * math.log(2),
      2 -> 2 * math.log(3),
      3 -> 2 * math.log(4),
      4 -> 2 * math.log(5)))
  }
}
