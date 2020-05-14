package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations._

class OneHotEncoderTest extends FunSuite with TestGraphOp {
  test("one-hot encode attribute", com.lynxanalytics.biggraph.SphynxOnly) {
    val graph = ExampleGraph()().result
    val categories = Seq("Female", "Male", "Non-binary")
    val oneHot = {
      val op = OneHotEncoder(categories)
      op(op.catAttr, graph.gender).result.oneHotVector
    }
    assert(oneHot.rdd.collect.toMap
      == Map(0 -> Vector(0, 1, 0), 1 -> Vector(1, 0, 0), 2 -> Vector(0, 1, 0), 3 -> Vector(0, 1, 0)))
  }
}
