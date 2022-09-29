package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class FindModularPartitioningTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val partitions = {
      val op = FindModularPartitioning()
      op(op.edges, eg.edges)(op.weights, eg.weight).result
    }
    val partitionMap = partitions.belongsTo.toPairSeq.toMap
    assert(partitionMap.values.toSet.size == 3)
    assert(partitionMap(0) == partitionMap(1))
  }
}
