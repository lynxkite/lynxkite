package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PSOGeneratorTest extends FunSuite with TestGraphOp {
  test("clustering strength test") {
    val vs = CreateVertexSet(1000)().result.vs
    val g = {
      val op = PSOGenerator(2, 2, 0.6, 1337)
      op(op.vs, vs).result
    }
    val out = {
      val op = ClusteringCoefficient()
      op(op.es, g.es).result
    }
    assert(out.clustering.rdd.map { case (_, clus) => clus }.reduce(_ + _) / 1000 > 0.65)
  }
}
