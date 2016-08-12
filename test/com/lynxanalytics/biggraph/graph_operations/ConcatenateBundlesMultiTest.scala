package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.language.implicitConversions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ConcatenateBundlesMultiTest extends FunSuite with TestGraphOp {
  def concatEdges(abSeq: Seq[(Seq[Int], Int)], bcSeq: Seq[(Seq[Int], Int)]): Seq[(Int, Int)] = {
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
    val acOp = ConcatenateBundlesMulti()
    val ac = acOp(
      acOp.edgesAB, ab.es)(
        acOp.edgesBC, bc.es).result

    // join edge bundle and weight data to make an output that is easy to read
    ac.edgesAC.rdd.map {
      case (id, edge) =>
        (edge.src.toInt, edge.dst.toInt)
    }.collect.toSeq.sortWith { sortTup }
  }

  val sortTup: ((Int, Int), (Int, Int)) => Boolean = { case ((s1, d1), (s2, d2)) => s1 < s2 || (s1 == s2 && d1 < d2) }

  implicit def toSeqMap(x: Seq[(Int, Int)]): Seq[(Seq[Int], Int)] = x.map { case (a, b) => Seq(a) -> b }

  test("no edge") {
    val AB = Seq(1 -> 10)
    val BC = Seq(20 -> 100)
    assert(concatEdges(AB, BC) === Seq())
  }

  test("isolated edges") {
    val AB = Seq(1 -> 10, 2 -> 20)
    val BC = Seq(10 -> 100, 20 -> 200)
    assert(concatEdges(AB, BC) === Seq((1, 100), (2, 200)).sortWith { sortTup })
  }

  test("one to many to one") {
    val AB = Seq(1 -> 10, 1 -> 20, 1 -> 30, 1 -> 40)
    val BC = Seq(Seq(10, 20, 30, 40) -> 100)
    assert(concatEdges(AB, BC) === Seq((1, 100), (1, 100), (1, 100), (1, 100)))
  }

  test("many to one to many") {
    val AB = Seq(Seq(1, 2, 3, 4) -> 10)
    val BC = Seq(10 -> 100, 10 -> 200, 10 -> 300, 10 -> 400)
    assert(concatEdges(AB, BC) === Seq(
      (1, 100), (1, 200), (1, 300), (1, 400),
      (2, 100), (2, 200), (2, 300), (2, 400),
      (3, 100), (3, 200), (3, 300), (3, 400),
      (4, 100), (4, 200), (4, 300), (4, 400)).sortWith { sortTup })
  }

  test("mix of the above") {
    val AB = Seq(Seq(1, 2) -> 10, Seq(1, 3) -> 20, Seq(4) -> 30)
    val BC = Seq(Seq(10, 20) -> 100, Seq(20) -> 200, Seq(30) -> 300, Seq(40) -> 400)
    assert(concatEdges(AB, BC) === Seq(
      (1, 100), (1, 100), (2, 100), (3, 100), (1, 200), (3, 200), (4, 300)).sortWith { sortTup })
  }
}
