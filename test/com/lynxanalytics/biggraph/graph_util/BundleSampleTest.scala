package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.ExampleGraph

class EdgeBundleSampleTest extends FunSuite with TestGraphOp {
  test("No sampling test") {
    val graph = ExampleGraph()().result
    val bs = new EdgeBundleSample(graph.edges, 10000)
    val resultRDD = bs.applyOp(
      graph.comment,
      SrcAttr(graph.name),
      SrcAttr(graph.age),
      DstAttr(graph.name)) {
        (comment, sname, sage, dname) => s"C: $comment sN: $sname sA: $sage dN: $dname"
      }
    assert(TestUtils.RDDToSortedString(resultRDD) ==
      """|(0,C: Adam loves Eve sN: Adam sA: 20.3 dN: Eve)
         |(1,C: Eve loves Adam sN: Eve sA: 18.2 dN: Adam)
         |(2,C: Bob envies Adam sN: Bob sA: 50.3 dN: Adam)
         |(3,C: Bob loves Eve sN: Bob sA: 50.3 dN: Eve)""".stripMargin)
  }
}

