package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import scala.language.implicitConversions

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class ConcatenateBundlesTest extends AnyFunSuite with TestGraphOp {
  def concatEdges(abSeq: Seq[(Seq[Int], Int)], bcSeq: Seq[(Seq[Int], Int)]): Map[(Int, Int), Double] = {
    val abES = abSeq.flatMap { case (s, i) => s.map(_.toLong -> i.toLong) }
    val bcES = bcSeq.flatMap { case (s, i) => s.map(_.toLong -> i.toLong) }
    val aVS = abES.map(_._1)
    val bVS = abES.map(_._2) ++ bcES.map(_._1)
    val cVS = bcES.map(_._2)
    // Create three vertex sets.
    val a = SmallTestGraph(aVS.map(_.toInt -> Seq()).toMap).result
    val b = SmallTestGraph(bVS.map(_.toInt -> Seq()).toMap).result
    val c = SmallTestGraph(cVS.map(_.toInt -> Seq()).toMap).result
    // Add two edge bundles.
    val abOp = AddWeightedEdges(abES, 1.0)
    val ab = abOp(abOp.src, a.vs)(abOp.dst, b.vs).result
    val bcOp = AddWeightedEdges(bcES, 1.0)
    val bc = bcOp(bcOp.src, b.vs)(bcOp.dst, c.vs).result
    // Concatenate!
    val cbOp = ConcatenateBundles()
    val cb = cbOp(
      cbOp.edgesAB,
      ab.es)(
      cbOp.weightsAB,
      ab.weight)(
      cbOp.edgesBC,
      bc.es)(
      cbOp.weightsBC,
      bc.weight).result

    // join edge bundle and weight data to make an output that is easy to read
    cb.edgesAC.rdd.join(cb.weightsAC.rdd).map {
      case (id, (edge, value)) =>
        (edge.src.toInt, edge.dst.toInt) -> value
    }.collect.toMap
  }

  implicit def toSeqMap(x: Seq[(Int, Int)]): Seq[(Seq[Int], Int)] = x.map { case (a, b) => Seq(a) -> b }

  test("no edge") {
    val AB = Seq(1 -> 10)
    val BC = Seq(20 -> 100)
    assert(concatEdges(AB, BC) === Map())
  }

  test("isolated edges") {
    val AB = Seq(1 -> 10, 2 -> 20)
    val BC = Seq(10 -> 100, 20 -> 200)
    assert(concatEdges(AB, BC) === Map((1, 100) -> 1.0, (2, 200) -> 1.0))
  }

  test("one to many to one") {
    val AB = Seq(1 -> 10, 1 -> 20, 1 -> 30, 1 -> 40)
    val BC = Seq(Seq(10, 20, 30, 40) -> 100)
    assert(concatEdges(AB, BC) === Map((1, 100) -> 4.0))
  }

  test("many to one to many") {
    val AB = Seq(Seq(1, 2, 3, 4) -> 10)
    val BC = Seq(10 -> 100, 10 -> 200, 10 -> 300, 10 -> 400)
    assert(concatEdges(AB, BC) === Map(
      (1, 100) -> 1.0,
      (1, 200) -> 1.0,
      (1, 300) -> 1.0,
      (1, 400) -> 1.0,
      (2, 100) -> 1.0,
      (2, 200) -> 1.0,
      (2, 300) -> 1.0,
      (2, 400) -> 1.0,
      (3, 100) -> 1.0,
      (3, 200) -> 1.0,
      (3, 300) -> 1.0,
      (3, 400) -> 1.0,
      (4, 100) -> 1.0,
      (4, 200) -> 1.0,
      (4, 300) -> 1.0,
      (4, 400) -> 1.0,
    ))
  }

  test("mix of the above") {
    val AB = Seq(Seq(1, 2) -> 10, Seq(1, 3) -> 20, Seq(4) -> 30)
    val BC = Seq(Seq(10, 20) -> 100, Seq(20) -> 200, Seq(30) -> 300, Seq(40) -> 400)
    assert(concatEdges(AB, BC) === Map(
      (1, 100) -> 2.0,
      (2, 100) -> 1.0,
      (3, 100) -> 1.0,
      (1, 200) -> 1.0,
      (3, 200) -> 1.0,
      (4, 300) -> 1.0))
  }
}
