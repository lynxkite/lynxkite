package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import org.apache.spark.SparkContext.rddToPairRDDFunctions

class MakePositionTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MakePosition()
    val res = op(op.attrA, g.age)(op.attrB, g.income).result.position
    assert(res.rdd.collect.toSeq == Seq(0 -> (20.3, 1000.0), 2 -> (50.3, 2000.0)))
  }
}
