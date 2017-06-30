package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

import com.lynxanalytics.biggraph.JavaScript

class ShortestPathTest extends FunSuite with TestGraphOp {

  test("big random graph") {
    val graph = CreateVertexSet(100)().result
    val vs = graph.vs
    val ordinal = graph.ordinal
    val es = {
      val op = FastRandomEdgeBundle(0, 6.0)
      op(op.vs, vs).result.es
    }
    val startingDistance = {
      val op = DeriveScala[Double](
        "if (ordinal < 3) Some(1000.0) else None",
        Seq("ordinal"))
      op(
        op.attrs,
        Seq(ordinal.asDouble.entity)).result.attr
    }
    val edgeDistance = AddConstantAttribute.run(es.idSet, 1.0)
    val distance = {
      val op = ShortestPath(3)
      op(op.vs, vs)(op.es, es)(op.edgeDistance, edgeDistance)(op.startingDistance, startingDistance).result.distance
    }
    val tooFarAway = distance.rdd.filter { case (_, distance) => distance > 1003.0 }.count()
    val farAway = distance.rdd.filter { case (_, distance) => distance > 1002.0 }.count()
    assert(tooFarAway == 0, s"${tooFarAway} nodes are further than max iterations")
    assert(farAway > 0, s"${farAway} nodes are found at distance 3")
  }

  test("one-line graph") {
    val graph = SmallTestGraph(
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(3),
        3 -> Seq(4),
        4 -> Seq(5),
        5 -> Seq())).result
    val startingDistance = AddVertexAttribute.run(graph.vs, Map(0 -> 100.0))
    val edgeDistance = AddConstantAttribute.run(graph.es.idSet, 2.0)
    val distance = {
      val op = ShortestPath(10)
      op(op.vs, graph.vs)(op.es, graph.es)(op.edgeDistance, edgeDistance)(op.startingDistance, startingDistance).result.distance
    }.rdd.collect
    assert(distance.toSeq.size == 6)
    assert(distance.toMap == Map(
      0 -> 100.0,
      1 -> 102.0,
      2 -> 104.0,
      3 -> 106.0,
      4 -> 108.0,
      5 -> 110.0))
  }

  test("graph with two paths with different weights") {
    val graph = SmallTestGraph(
      Map(
        0 -> Seq(1, 5),
        1 -> Seq(2),
        2 -> Seq(3),
        3 -> Seq(4),
        5 -> Seq(6),
        6 -> Seq(4))).result
    val startingDistance = AddVertexAttribute.run(graph.vs, Map(0 -> 0.0))
    val edgeDistance = {
      val vertexWeights = AddVertexAttribute.run(graph.vs, Map(
        0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0, 5 -> 10.0, 6 -> 10.0))
      VertexToEdgeAttribute.srcAttribute(vertexWeights, graph.es)
    }
    val distance = {
      val op = ShortestPath(10)
      op(op.vs, graph.vs)(op.es, graph.es)(op.edgeDistance, edgeDistance)(op.startingDistance, startingDistance).result.distance
    }.rdd.collect

    assert(distance.toMap.get(4).get == 4.0)
    assert(distance.toMap.get(6).get == 11.0)
  }

  test("example graph") {
    val graph = ExampleGraph()().result
    val vs = graph.vertices
    val es = graph.edges
    val name = graph.name
    val startingDistance = {
      val op = DeriveScala[Double](
        "if (name == \"Bob\") Some(1000.0) else None",
        Seq("name"))
      op(
        op.attrs,
        Seq(name.entity)).result.attr
    }
    val distance = {
      val op = ShortestPath(10)
      op(op.vs, vs)(op.es, es)(op.edgeDistance, graph.weight)(op.startingDistance, startingDistance).result.distance
    }
    val nameAndDistance = name.rdd.join(distance.rdd).collect
    assert(nameAndDistance.size == 3)
    assert(nameAndDistance.toMap == Map(
      0 -> ("Adam", 1003.0),
      1 -> ("Eve", 1004.0),
      2 -> ("Bob", 1000.0)))
  }
}
