package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PSOGeneratorTest extends FunSuite with TestGraphOp {
  test("clustering strength test") {
    val g = PSOGenerator(1000, 2, 2, 0.6, 1337)().result
    val op = ClusteringCoefficient()
    val out = op(op.vs, g.vs)(op.es, g.es).result
    assert(out.clustering.rdd.map { case (_, clus) => clus }.reduce(_ + _) / 1000 > 0.65)
  }
}
