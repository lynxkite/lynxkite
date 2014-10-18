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
    assert(fingerprint(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(4, 5, 6)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(4, 5, 6)))
      == Set(10 -> 20, 11 -> 21))
  }

  test("one difficult pair A") {
    assert(fingerprint(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3, 4)))
      == Set(11 -> 20))
  }

  test("one difficult pair B") {
    assert(fingerprint(
      Map(10 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(1, 2, 3, 4)))
      == Set(10 -> 21))
  }

  test("no match") {
    assert(fingerprint(
      Map(10 -> Seq(1, 2, 3)),
      Map(20 -> Seq(4, 5, 6)))
      == Set())
  }

  def fingerprint(left: Map[Int, Seq[Int]], right: Map[Int, Seq[Int]]): Set[(Int, Int)] = {
    val graph = SmallTestGraph(left ++ right).result
    val leftName = {
      val op = AddVertexAttribute(left.map { case (k, v) => k -> s"L$k" })
      op(op.vs, graph.vs).result.attr
    }
    val rightName = {
      val op = AddVertexAttribute(right.map { case (k, v) => k -> s"R$k" })
      op(op.vs, graph.vs).result.attr
    }
    val weight = AddConstantAttribute.run(graph.es.asVertexSet, 1.0)
    val candidates = {
      val op = AddEdgeBundle(for { l <- left.keys.toSeq; r <- right.keys.toSeq } yield l -> r)
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
    fingerprinting.leftToRight.toPairSet.map { case (l, r) => (l.toInt, r.toInt) }
  }
}
