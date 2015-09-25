package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FEBucketersTest extends FunSuite with TestGraphOp {

  def histogramTests(sampleSize: => Int) {
    test("rare string values are not counted, sampleSize= " + sampleSize) {
      val g = ExampleGraph()().result
      val bucketed = BucketedAttribute(g.name, StringBucketer(Seq("Bob", "Eve"), false))
      val counts = bucketed.toHistogram(g.vertices, sampleSize).counts.value
      assert(counts == Map(0 -> 1, 1 -> 1))
    }
  }

  testsFor(histogramTests(50000));
  testsFor(histogramTests(-1));
}
