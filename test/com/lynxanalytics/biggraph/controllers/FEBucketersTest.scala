package com.lynxanalytics.biggraph.controllers

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FEBucketersTest extends AnyFunSuite with TestGraphOp {
  test("rare string values are not counted") {
    val g = ExampleGraph()().result
    val bucketed = BucketedAttribute(g.name, StringBucketer(Seq("Bob", "Eve"), false))
    val counts = bucketed.toHistogram(g.vertices, 50000).counts.value
    assert(counts == Map(0 -> 1, 1 -> 1))
  }
}
