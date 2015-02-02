package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class FindModularPartitioningTest extends FunSuite with TestGraphOp {
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
