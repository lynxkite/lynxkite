package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class FindModularClusteringByTweaksTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val clusters = {
      val op = FindModularClusteringByTweaks()
      op(op.edges, eg.edges)(op.weights, eg.weight).result
    }
    val clusterMap = clusters.belongsTo.toPairSeq.toMap
    assert(clusterMap.values.toSet.size == 2)
    assert(clusterMap(0) == clusterMap(1))
    assert(clusterMap(0) == clusterMap(2))
  }
}
