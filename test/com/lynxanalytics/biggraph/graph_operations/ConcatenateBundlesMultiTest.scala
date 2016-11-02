package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.language.implicitConversions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ConcatenateBundlesMultiTest extends FunSuite with TestGraphOp {
  def concatEdges(abSeq: Seq[(Int, Int)], bcSeq: Seq[(Int, Int)]): Seq[(Int, Int)] = {
    val abES = abSeq.map { case (a, b) => a.toLong -> b.toLong }
    val bcES = bcSeq.map { case (a, b) => a.toLong -> b.toLong }
    val aVS = abSeq.map(_._1)
    val bVS = abSeq.map(_._2) ++ bcSeq.map(_._1)
    val cVS = bcSeq.map(_._2)
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
    val acOp = ConcatenateBundlesMulti()
    val ac = acOp(
      acOp.edgesAB, ab.es)(
        acOp.edgesBC, bc.es).result

    // create readable output
    ac.edgesAC.rdd.map {
      case (id, edge) =>
        (edge.src.toInt, edge.dst.toInt)
    }.collect.toSeq.sorted
  }

  test("no edge") {
    val ab = Seq(1 -> 10)
    val bc = Seq(20 -> 100)
    assert(concatEdges(ab, bc) === Seq())
  }

  test("isolated edges") {
    val ab = Seq(1 -> 10, 2 -> 20)
    val bc = Seq(10 -> 100, 20 -> 200)
    assert(concatEdges(ab, bc) === Seq(1 -> 100, 2 -> 200).sorted)
  }

  test("one to many to one") {
    val ab = Seq(1 -> 10, 1 -> 20, 1 -> 30, 1 -> 40)
    val bc = Seq(10 -> 100, 20 -> 100, 30 -> 100, 40 -> 100)
    assert(concatEdges(ab, bc) === Seq(1 -> 100, 1 -> 100, 1 -> 100, 1 -> 100).sorted)
  }

  test("many to one to many") {
    val ab = Seq(1 -> 10, 2 -> 10, 3 -> 10, 4 -> 10)
    val bc = Seq(10 -> 100, 10 -> 200, 10 -> 300, 10 -> 400)
    assert(concatEdges(ab, bc) === Seq(
      (1, 100), (1, 200), (1, 300), (1, 400),
      (2, 100), (2, 200), (2, 300), (2, 400),
      (3, 100), (3, 200), (3, 300), (3, 400),
      (4, 100), (4, 200), (4, 300), (4, 400)).sorted)
  }

  test("mix of the above") {
    val ab = Seq(1 -> 10, 2 -> 10, 1 -> 20, 3 -> 20, 4 -> 30)
    val bc = Seq(10 -> 100, 20 -> 100, 20 -> 200, 30 -> 300, 40 -> 400)
    assert(concatEdges(ab, bc) === Seq(
      (1, 100), (1, 100), (2, 100), (3, 100), (1, 200), (3, 200), (4, 300)).sorted)
  }
}
