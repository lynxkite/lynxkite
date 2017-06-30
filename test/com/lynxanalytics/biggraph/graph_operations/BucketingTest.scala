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
      val op = DeriveJS[Double](
        "age - 20",
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

  def getSegmentsWithSizes(bucketing: IntervalBucketing.Output): Seq[((Double, Double), Int)] = {
    val segmentSizes = bucketing
      .belongsTo
      .rdd
      .map { case (_, edge) => (edge.dst, 1) }
      .reduceByKey(_ + _)
    segmentSizes
      .join(bucketing.bottom.rdd.join(bucketing.top.rdd))
      .collect
      .toSeq
      .map { case (_, (size, (bottom, top))) => ((bottom, top), size) }
      .sorted
  }

  test("example graph by age intervals") {
    val g = ExampleGraph()().result
    val ageTimes1_5 = {
      val op = DeriveJS[Double](
        "age * 1.5",
        Seq("age"))
      op(
        op.attrs,
        VertexAttributeToJSValue.seq(g.age)).result.attr
    }
    // intervals should be: [2, 3], [18.2, 27.3], [20.3, 30.45], [50.3, 75.45]
    val bucketing = {
      val op = IntervalBucketing(bucketWidth = 10.0, overlap = false)
      op(op.beginAttr, g.age)(op.endAttr, ageTimes1_5).result
    }
    assert(getSegmentsWithSizes(bucketing) == Seq(
      ((0.0, 10.0), 1),
      ((10.0, 20.0), 1),
      ((20.0, 30.0), 2),
      ((30.0, 40.0), 1),
      ((50.0, 60.0), 1),
      ((60.0, 70.0), 1),
      ((70.0, 80.0), 1)))
  }

  test("example graph by age intervals with overlap") {
    val g = ExampleGraph()().result
    val ageTimes1_5 = {
      val op = DeriveJS[Double](
        "age * 1.5",
        Seq("age"))
      op(
        op.attrs,
        VertexAttributeToJSValue.seq(g.age)).result.attr
    }
    // intervals should be: [2, 3], [18.2, 27.3], [20.3, 30.45], [50.3, 75.45]
    val bucketing = {
      val op = IntervalBucketing(bucketWidth = 10.0, overlap = true)
      op(op.beginAttr, g.age)(op.endAttr, ageTimes1_5).result
    }
    assert(getSegmentsWithSizes(bucketing) == Seq(
      ((-5.0, 5.0), 1),
      ((0.0, 10.0), 1),
      ((10.0, 20.0), 1),
      ((15.0, 25.0), 2),
      ((20.0, 30.0), 2),
      ((25.0, 35.0), 2),
      ((30.0, 40.0), 1),
      ((45.0, 55.0), 1),
      ((50.0, 60.0), 1),
      ((55.0, 65.0), 1),
      ((60.0, 70.0), 1),
      ((65.0, 75.0), 1),
      ((70.0, 80.0), 1),
      ((75.0, 85.0), 1)))
  }
}
