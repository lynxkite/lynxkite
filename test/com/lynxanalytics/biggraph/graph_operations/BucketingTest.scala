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
  }

  test("example graph by age with spread") {
    val g = ExampleGraph()().result
    val minmax = {
      val op = ComputeMinMaxDouble()
      op(op.attribute, g.age).result
    }
    val bucketing = {
      val op = FixedWidthDoubleBucketing(bucketWidth = 10.0, spread = 1)
      op(op.attr, g.age)(op.min, minmax.min)(op.max, minmax.max).result
    }
    assert(bucketing.segments.toSeq.size == 7)
    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 1, 1, 1, 2, 3, 3))
    assert(segmentSizes.sum == 4 * 3)
  }
}
