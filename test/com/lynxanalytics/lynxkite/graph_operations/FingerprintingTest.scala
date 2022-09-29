package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import scala.language.implicitConversions

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class FingerprintingTest extends AnyFunSuite with TestGraphOp {
  test("two easy pairs") {
    val f = new Fingerprint(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(4, 5, 6)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(4, 5, 6)))
    assert(f.matching == Seq(10 -> 20, 11 -> 21))
    assert(f.similarities == Map(10 -> 1.0, 11 -> 1.0))
  }

  test("one difficult pair A") {
    val f = new Fingerprint(
      Map(10 -> Seq(1, 2, 3, 10, 11), 11 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3, 4)))
    assert(f.matching == Seq(11 -> 20))
  }

  test("one difficult pair B") {
    val f = new Fingerprint(
      Map(10 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3, 20, 21), 21 -> Seq(1, 2, 3, 4)))
    assert(f.matching == Seq(10 -> 21))
  }

  test("bad match") {
    val f = new Fingerprint(
      Map(10 -> Seq(1, 2, 3)),
      Map(20 -> Seq(4, 5, 6)))
    assert(f.matching == Seq(10 -> 20))
    assert(f.similarities(10) == 0)
  }

  test("isolated points") {
    val f = new Fingerprint(
      Map(10 -> Seq()),
      Map(20 -> Seq()))
    assert(f.matching == Seq(10 -> 20))
    assert(f.similarities(10) == 0)
  }

  class Fingerprint(left: Map[Int, Seq[Int]], right: Map[Int, Seq[Int]]) {
    val graph = SmallTestGraph(left ++ right).result
    val weights = AddConstantAttribute.run(graph.es.idSet, 1.0)
    val candidates = {
      val op = AddEdgeBundle(for { l <- left.keys.toSeq; r <- right.keys.toSeq } yield l -> r)
      op(op.vsA, graph.vs)(op.vsB, graph.vs).result.esAB
    }
    val fingerprinting = {
      val op = Fingerprinting(0, 0)
      op(
        op.leftEdges,
        graph.es)(
        op.leftEdgeWeights,
        weights)(
        op.rightEdges,
        graph.es)(
        op.rightEdgeWeights,
        weights)(
        op.candidates,
        candidates).result
    }
    val matching = fingerprinting.matching.toPairSeq.map { case (l, r) => (l.toInt, r.toInt) }
    val similarities = fingerprinting.leftSimilarities.rdd.collect.toMap
  }
}

class FingerprintingCandidatesTest extends AnyFunSuite with TestGraphOp {
  test("two pairs") {
    assert(candidates(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(4, 5, 6)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(4, 5, 6)))
      == Seq(10 -> 20, 11 -> 21))
  }

  test("two left, one right") {
    assert(candidates(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3, 4)))
      == Seq(10 -> 20, 11 -> 20))
  }

  test("one left, two right") {
    assert(candidates(
      Map(10 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(1, 2, 3, 4)))
      == Seq(10 -> 20, 10 -> 21))
  }

  test("two left, two right") {
    assert(candidates(
      Map(10 -> Seq(1, 2, 3), 11 -> Seq(1, 2, 3, 4)),
      Map(20 -> Seq(1, 2, 3), 21 -> Seq(1, 2, 3, 4)))
      == Seq(10 -> 20, 10 -> 21, 11 -> 20, 11 -> 21))
  }

  test("no match") {
    assert(candidates(
      Map(10 -> Seq(1, 2, 3)),
      Map(20 -> Seq(4, 5, 6)))
      == Seq())
  }

  def candidates(left: Map[Int, Seq[Int]], right: Map[Int, Seq[Int]]): Seq[(Int, Int)] = {
    val graph = SmallTestGraph(left ++ right).result
    val leftName = AddVertexAttribute.run(
      graph.vs,
      (left.keys ++ left.values.flatten).map(i => i -> s"L$i").toMap)
    val rightName = AddVertexAttribute.run(
      graph.vs,
      (right.keys ++ right.values.flatten).map(i => i -> s"R$i").toMap)
    val candidates = {
      val op = FingerprintingCandidates()
      op(op.es, graph.es)(op.leftName, leftName)(op.rightName, rightName).result.candidates
    }
    candidates.toPairSeq.map { case (l, r) => (l.toInt, r.toInt) }
  }
}
