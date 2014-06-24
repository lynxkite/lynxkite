package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import scala.language.implicitConversions

import com.lynxanalytics.biggraph.graph_api._

class ConcatenateBundlesTest extends FunSuite with TestGraphOperation {
  def concatEdges(abSeq: Seq[(Seq[Int], Int)], bcSeq: Seq[(Seq[Int], Int)]): Map[(Int, Int), Double] = {
    val abES = abSeq.flatMap { case (s, i) => s.map(_.toLong -> i.toLong) }
    val bcES = bcSeq.flatMap { case (s, i) => s.map(_.toLong -> i.toLong) }
    val aVS = abES.map(_._1)
    val bVS = abES.map(_._2) ++ bcES.map(_._1)
    val cVS = bcES.map(_._2)
    // Create three vertex sets.
    val a = helper.apply(SmallTestGraph(aVS.map(_.toInt -> Seq()).toMap))
    val b = helper.apply(SmallTestGraph(bVS.map(_.toInt -> Seq()).toMap))
    val c = helper.apply(SmallTestGraph(cVS.map(_.toInt -> Seq()).toMap))
    // Add two edge bundles.
    val ab = helper.apply(AddWeightedEdges(abES, 1.0), 'src -> a.vertexSets('vs), 'dst -> b.vertexSets('vs))
    val bc = helper.apply(AddWeightedEdges(bcES, 1.0), 'src -> b.vertexSets('vs), 'dst -> c.vertexSets('vs))
    // Concatenate!
    val cb = helper.apply(
      ConcatenateBundles(),
      'weightsAB -> ab.edgeAttributes('weight),
      'weightsBC -> bc.edgeAttributes('weight))
    helper.localData(cb.edgeAttributes('weightsAC).runtimeSafeCast[Double])
      .map { case ((a, b), c) => ((a.toInt, b.toInt), c) }
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
      (1, 100) -> 1.0, (1, 200) -> 1.0, (1, 300) -> 1.0, (1, 400) -> 1.0,
      (2, 100) -> 1.0, (2, 200) -> 1.0, (2, 300) -> 1.0, (2, 400) -> 1.0,
      (3, 100) -> 1.0, (3, 200) -> 1.0, (3, 300) -> 1.0, (3, 400) -> 1.0,
      (4, 100) -> 1.0, (4, 200) -> 1.0, (4, 300) -> 1.0, (4, 400) -> 1.0))
  }

  test("mix of the above") {
    val AB = Seq(Seq(1, 2) -> 10, Seq(1, 3) -> 20, Seq(4) -> 30)
    val BC = Seq(Seq(10, 20) -> 100, Seq(20) -> 200, Seq(30) -> 300, Seq(40) -> 400)
    assert(concatEdges(AB, BC) === Map(
      (1, 100) -> 2.0,
      (2, 100) -> 1.0, (3, 100) -> 1.0, (1, 200) -> 1.0, (3, 200) -> 1.0, (4, 300) -> 1.0))
  }
}
