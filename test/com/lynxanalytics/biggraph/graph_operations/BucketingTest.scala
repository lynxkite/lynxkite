package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util._

class BucketingTest extends FunSuite with TestGraphOp {
  test("example graph by gender") {
    val g = ExampleGraph()().result
    val bucketing = {
      val op = StringBucketing()
      op(op.attr, g.gender).result
    }
    assert(bucketing.segments.toSeq.size == 2)
    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 3))
    assert(bucketing.label.rdd.values.collect.toSeq.sorted == Seq("Female", "Male"))
  }

  test("example graph by age with overlap") {
    val g = ExampleGraph()().result
    val bucketing = {
      val op = DoubleBucketing(bucketWidth = 20.0, overlap = true)
      op(op.attr, g.age).result
    }
    assert(bucketing.segments.toSeq.size == 6)
    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 1, 1, 1, 2, 2))
    assert(segmentSizes.sum == 4 * 2)
    assert(bucketing.label.rdd.values.collect.toSeq.sorted ==
      Seq("-10.0 to 10.0", "0.0 to 20.0", "10.0 to 30.0", "20.0 to 40.0", "40.0 to 60.0", "50.0 to 70.0"))
  }
}
