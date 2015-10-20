package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.JavaScript

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
    assert(bucketing.bottom.rdd.values.collect.toSeq.sorted ==
      Seq(-10.0, 0.0, 10.0, 20.0, 40.0, 50.0))
    assert(bucketing.top.rdd.values.collect.toSeq.sorted ==
      Seq(10.0, 20.0, 30.0, 40.0, 60.0, 70.0))
  }

  test("example graph with negative values") {
    val g = ExampleGraph()().result
    val ageMinus20 = {
      val op = DeriveJSDouble(
        JavaScript("age - 20"),
        Seq("age"))
      op(
        op.attrs,
        VertexAttributeToJSValue.seq(g.age)).result.attr
    }
    // ages should be: -18, -1.8, 0.3, 30.3
    val bucketing = {
      val op = DoubleBucketing(bucketWidth = 10.0, overlap = false)
      op(op.attr, ageMinus20).result
    }
    assert(bucketing.segments.toSeq.size == 4)
    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 1, 1, 1))
    assert(bucketing.bottom.rdd.values.collect.toSeq.sorted ==
      Seq(-20.0, -10.0, 0.0, 30.0))
    assert(bucketing.top.rdd.values.collect.toSeq.sorted ==
      Seq(-10.0, 0.0, 10.0, 40.0))
  }
}
