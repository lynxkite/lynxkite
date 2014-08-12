package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import org.apache.spark.SparkContext.rddToPairRDDFunctions

class EdgesFromAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = EdgesFromAttributeMatches[String]()
    val res = op(op.attr, g.gender).result
    val edges = res.edges.rdd.collect.toSeq.map { case (id, edge) => edge.src -> edge.dst }.toSet
    assert(edges == Set(0 -> 2, 0 -> 3, 2 -> 0, 2 -> 3, 3 -> 0, 3 -> 2))
  }
}
