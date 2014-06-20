package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.rdd
import org.scalatest.FunSuite
import scala.util.Random

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class SetOverlapForCCTest extends FunSuite with TestGraphOperation {
  def RandomSets(esize: Int, vsize: Int, seed: Int): Seq[(Seq[Int], Int)] = {
    val rand = new Random(seed)
    val elementIds = Seq.range[Int](0, vsize)

    (0 until esize).map { id =>
      (rand.shuffle(elementIds.toSeq).take(rand.nextInt(elementIds.size + 1)), id)
    }
  }

  test("Check for a few random sets") {
    val rnd = new Random(0)
    val trials = 20
    for (i <- (0 until trials)) {
      val eSize = rnd.nextInt(100)
      val vSize = rnd.nextInt(30) + 1
      val seed = rnd.nextInt()
      val minOverlap = rnd.nextInt(6) + 1
      val (vs, sets, links, _) = helper.groupedGraph(RandomSets(eSize, vSize, seed))

      // this is a slow test so lets inform the tester about what is going on
      println(s"Checking graph ${i + 1}/$trials, parameters: $eSize, $vSize, $seed, overlap: $minOverlap")

      val SOnormal = helper.apply(SetOverlap(minOverlap), 'vs -> vs, 'sets -> sets, 'links -> links)
      val SOforCC = helper.apply(UniformOverlapForCC(minOverlap), 'vs -> vs, 'sets -> sets, 'links -> links)
      val CCnormal = helper.apply(ConnectedComponents(), 'vs -> sets, 'es -> SOnormal.edgeBundles('overlaps))
      val CCforCC = helper.apply(ConnectedComponents(), 'vs -> sets, 'es -> SOforCC.edgeBundles('overlaps))

      ConnectedComponentsTest.assertSameComponents(
        helper.localData(CCnormal.edgeBundles('links)).toMap,
        helper.localData(CCnormal.edgeBundles('links)).toMap)
    }
  }
}
