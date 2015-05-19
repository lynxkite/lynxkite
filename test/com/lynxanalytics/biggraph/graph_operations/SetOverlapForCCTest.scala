package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SetOverlapForCCTest extends FunSuite with TestGraphOp {
  def RandomSets(esize: Int, vsize: Int, seed: Int): Seq[(Seq[Int], Int)] = {
    val rand = new Random(seed)
    val elementIds = Seq.range[Int](0, vsize)

    (0 until esize).map { id =>
      (rand.shuffle(elementIds.toSeq).take(rand.nextInt(elementIds.size + 1)), id)
    }
  }

  test("Check for a few random sets") {
    val rnd = new Random(0)
    val trials = 2 // Increase when you need a more thorough test.
    for (i <- (0 until trials)) {
      val eSize = rnd.nextInt(100)
      val vSize = rnd.nextInt(30) + 1
      val seed = rnd.nextInt()
      val minOverlap = rnd.nextInt(6) + 1
      val g = SegmentedTestGraph(RandomSets(eSize, vSize, seed)).result

      // this is a slow test so lets inform the tester about what is going on
      println(s"Checking graph ${i + 1}/$trials, parameters: $eSize, $vSize, $seed, overlap: $minOverlap")

      val so = {
        val op = SetOverlap(minOverlap)
        op(op.belongsTo, g.belongsTo).result
      }
      val so4cc = {
        val op = UniformOverlapForCC(minOverlap)
        op(op.belongsTo, g.belongsTo).result
      }
      val op = ConnectedComponents()
      val cc = op(op.es, so.overlaps).result
      val cc4cc = op(op.es, so4cc.overlaps).result

      ConnectedComponentsTest.assertSameComponents(
        cc.belongsTo.toPairSeq.toMap,
        cc4cc.belongsTo.toPairSeq.toMap)
    }
  }
}
