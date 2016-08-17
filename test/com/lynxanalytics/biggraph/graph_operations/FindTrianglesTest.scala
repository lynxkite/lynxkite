package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import scala.collection.mutable
import scala.util.Random

class FindTrianglesTest extends FunSuite with TestGraphOp {
  test("no triangles 1") {
    val g = SmallTestGraph(Map(
      0 -> Seq(0, 0, 1, 1, 2),
      1 -> Seq(),
      2 -> Seq(0),
      3 -> Seq(4),
      4 -> Seq(5),
      5 -> Seq(6),
      6 -> Seq(3)
    )).result
    val op = FindTrianglesNew(needsBothDirections = false)
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(ftOut.segments.rdd.count == 0)
  }

  test("no triangles 2") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1),
      1 -> Seq(0, 2, 2),
      2 -> Seq(0, 0, 1)
    )).result
    val op = FindTrianglesNew(needsBothDirections = true)
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(ftOut.segments.rdd.count == 0)
  }

  test("ignore multiple edges") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1, 1, 1, 1, 2),
      1 -> Seq(2, 2, 2, 0, 0),
      2 -> Seq(0, 0, 1, 1)
    )).result
    val opF = FindTrianglesNew(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTrianglesNew(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert((ftFOut.segments.rdd.count, ftTOut.segments.rdd.count) == (1, 1))
  }

  test("5-size clique") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2, 3, 4),
      1 -> Seq(2, 3, 4),
      2 -> Seq(3, 4),
      3 -> Seq(4),
      4 -> Seq()
    )).result
    val opF = FindTrianglesNew(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTrianglesNew(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert((ftFOut.segments.rdd.count, ftTOut.segments.rdd.count) == (10, 0))
  }

  test("5-size clique both directions") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2, 3, 4),
      1 -> Seq(0, 2, 3, 4),
      2 -> Seq(0, 1, 3, 4),
      3 -> Seq(0, 1, 2, 4),
      4 -> Seq(0, 1, 2, 3)
    )).result
    val opF = FindTrianglesNew(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTrianglesNew(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert((ftFOut.segments.rdd.count, ftTOut.segments.rdd.count) == (10, 10))
  }

  test("planar graph neighbouring triangles") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2),
      1 -> Seq(0, 2, 3),
      2 -> Seq(0, 1, 3),
      3 -> Seq(1, 2, 4, 5),
      4 -> Seq(3, 5),
      5 -> Seq(3, 4)
    )).result
    val opF = FindTrianglesNew(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTrianglesNew(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert((ftFOut.segments.rdd.count, ftTOut.segments.rdd.count) == (3, 3))
  }

  ignore("performance test") {
    println("[info] Performance test started")
    testPerformance(1000, 0.9, 5, 100)
    testPerformance(10000, 0.9, 5, 100)
    testPerformance(100000, 0.9, 5, 100)
    testPerformance(1000000, 0.9, 5, 100)
    testPerformance(10000, 0.9, 15, 1000)
    testPerformance(100000, 0.9, 15, 1000)
    testPerformance(100000, 0.7, 10, 200)
    println("[info] PerformanceNew test started")
    testPerformanceNew(1000, 0.9, 5, 100)
    testPerformanceNew(10000, 0.9, 5, 100)
    testPerformanceNew(100000, 0.9, 5, 100)
    testPerformanceNew(1000000, 0.9, 5, 100)
    testPerformanceNew(10000, 0.9, 15, 1000)
    testPerformanceNew(100000, 0.9, 15, 1000)
    testPerformanceNew(100000, 0.7, 10, 200)
    //assert(true)
  }

  def testPerformance(n: Int,
                      ratio: Double,
                      lowDegree: Int,
                      highDegree: Int): Unit = {
    val random = new Random(19910306)
    val adjacencyArray = mutable.Map[Int, Seq[Int]]()
    for (i <- 1 to n) {
      val maxdegree = if (i < n * ratio) lowDegree else highDegree
      val degree = random.nextInt(maxdegree)
      val neighbours = mutable.ArrayBuffer[Int]()
      for (j <- 1 to degree) {
        neighbours += random.nextInt(n)
      }
      adjacencyArray += (i -> neighbours)
    }
    val g = SmallTestGraph(adjacencyArray.toMap).result
    val t0 = System.nanoTime()
    val op = FindTriangles(needsBothDirections = false)
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    print("[info] - " + ftOut.segments.rdd.count + " triangles found in ")
    val t1 = System.nanoTime()
    println((t1 - t0) / 1000000000.0 + " seconds")
  }

  def testPerformanceNew(n: Int,
                         ratio: Double,
                         lowDegree: Int,
                         highDegree: Int): Unit = {
    val random = new Random(19910306)
    val adjacencyArray = mutable.Map[Int, Seq[Int]]()
    for (i <- 1 to n) {
      val maxdegree = if (i < n * ratio) lowDegree else highDegree
      val degree = random.nextInt(maxdegree)
      val neighbours = mutable.ArrayBuffer[Int]()
      for (j <- 1 to degree) {
        neighbours += random.nextInt(n)
      }
      adjacencyArray += (i -> neighbours)
    }
    val g = SmallTestGraph(adjacencyArray.toMap).result
    val t0 = System.nanoTime()
    val op = FindTrianglesNew(needsBothDirections = false)
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    print("[info] - " + ftOut.segments.rdd.count + " triangles found in ")
    val t1 = System.nanoTime()
    println((t1 - t0) / 1000000000.0 + " seconds")
  }
}
