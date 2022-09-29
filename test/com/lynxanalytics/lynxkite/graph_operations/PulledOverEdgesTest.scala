package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class PulledOverEdgesTest extends AnyFunSuite with TestGraphOp {
  test("works with filters") {
    val g = ExampleGraph()().result
    implicit val d = SerializableType.double
    val fop = VertexAttributeFilter(GT(2.0))
    val fopRes = fop(fop.attr, g.weight).result

    val pop = PulledOverEdges()
    val pulledEB =
      pop(pop.injection, fopRes.identity)(pop.originalEB, g.edges).result.pulledEB

    assert(pulledEB.toPairSeq == Seq((2L, 0L), (2L, 1L)))
  }
}
