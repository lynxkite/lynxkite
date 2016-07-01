package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ReduceDimensionsTest extends FunSuite with TestGraphOp {

  test("example graph") {
    val g = ExampleGraph()().result
    val op = ReduceDimensions(2)
    val result = op(op.features, Seq(g.age, g.income): Seq[Attribute[Double]]).result
    val attr1 = result.attr1.rdd
    val attr2 = result.attr2.rdd
    assert(attr1.count == 2)
    assert(attr2.count == 2)
  }

  test("larger data set with 40 attributes") {
    val numAttr = 40
    val attrs = (1 to numAttr).map(i => (0 to 1000).map {
      case x => x -> (x * i).toDouble
    }.toMap)
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq()), 10).result
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ReduceDimensions(numAttr)
    val result = op(op.features, features).result
    val attr1 = result.attr1.rdd
    val attr2 = result.attr2.rdd
    assert((attr1.lookup(500)(0) - 0.0).abs <= 1E-6,
      "the principal component shall center at the origin")
    assert(attr1.partitioner.get.numPartitions == 10,
      "numbers of partitions shall remain the same")
    assert(attr1.count == 1001)
    assert(attr2.count == 1001)
  }
}
