package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_operations._

class OneHotEncoderTest extends AnyFunSuite with TestGraphOp {
  test("one-hot encode attribute", com.lynxanalytics.lynxkite.SphynxOnly) {
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
