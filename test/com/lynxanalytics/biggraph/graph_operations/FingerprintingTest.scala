package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.scalatest.FunSuite

import scala.util.Random
import scala.language.implicitConversions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class FingerprintingTest extends FunSuite with TestGraphOp {
  test("two easy pairs") {
    val graph = SmallTestGraph(Map(
      10 -> Seq(1, 2, 3), 11 -> Seq(4, 5, 6), 20 -> Seq(1, 2, 3), 21 -> Seq(4, 5, 6))).result
    val leftName = {
      val op = AddVertexAttribute(Map(
        1 -> "L1", 2 -> "L2", 3 -> "L3", 4 -> "L4", 5 -> "L5", 6 -> "L6", 10 -> "L10", 11 -> "L11"))
      op(op.vs, graph.vs).result.attr
    }
    val rightName = {
      val op = AddVertexAttribute(Map(
        1 -> "R1", 2 -> "R2", 3 -> "R3", 4 -> "R4", 5 -> "R5", 6 -> "R6", 20 -> "R20", 21 -> "R21"))
      op(op.vs, graph.vs).result.attr
    }
    val weight = AddConstantAttribute.run(graph.es.asVertexSet, 1.0)
    val candidates = {
      val op = AddEdgeBundle(Seq(10 -> 20, 10 -> 21, 11 -> 20, 11 -> 21))
      op(op.vsA, graph.vs)(op.vsB, graph.vs).result.esAB
    }
    val fingerprinting = {
      val op = Fingerprinting(1, 0)
      op(
        op.es, graph.es)(
          op.weight, weight)(
            op.leftName, leftName)(
              op.rightName, rightName)(
                op.candidates, candidates).result
    }
    assert(fingerprinting.leftToRight.toPairSet == Set(10L -> 20L, 11L -> 21L))
  }

  test("one difficult pair") {
    val graph = SmallTestGraph(Map(
      10 -> Seq(1, 2, 3), 11 -> Seq(1, 2, 3, 4), 20 -> Seq(1, 2, 3, 4))).result
    val leftName = {
      val op = AddVertexAttribute(Map(
        1 -> "L1", 2 -> "L2", 3 -> "L3", 4 -> "L4", 10 -> "L10", 11 -> "L11"))
      op(op.vs, graph.vs).result.attr
    }
    val rightName = {
      val op = AddVertexAttribute(Map(
        1 -> "R1", 2 -> "R2", 3 -> "R3", 4 -> "R4", 20 -> "L20"))
      op(op.vs, graph.vs).result.attr
    }
    val weight = AddConstantAttribute.run(graph.es.asVertexSet, 1.0)
    val candidates = {
      val op = AddEdgeBundle(Seq(10 -> 20, 11 -> 20))
      op(op.vsA, graph.vs)(op.vsB, graph.vs).result.esAB
    }
    val fingerprinting = {
      val op = Fingerprinting(1, 0)
      op(
        op.es, graph.es)(
          op.weight, weight)(
            op.leftName, leftName)(
              op.rightName, rightName)(
                op.candidates, candidates).result
    }
    assert(fingerprinting.leftToRight.toPairSet == Set(11L -> 20L))
  }
}
