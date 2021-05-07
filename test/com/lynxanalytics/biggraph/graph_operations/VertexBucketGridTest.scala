package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_util._

class VertexBucketGridTest extends AnyFunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("Only 1 bucket") {
    val xBucketer = new EmptyBucketer()
    val yBucketer = new EmptyBucketer()
    val count = Count.run(g.vertices)
    val op = VertexBucketGrid[Nothing, Nothing](xBucketer, yBucketer, 50000)
    val out = op(op.vertices, g.vertices)(op.filtered, g.vertices)(op.originalCount, count).result
    assert(out.buckets.value.counts == Map((0, 0) -> 4))
  }

  test("String-Double bucketing") {
    val numBuckets = 2
    val xBucketer = StringBucketer(Seq("Adam", "Eve"), hasOther = true)
    val yBucketer = DoubleLinearBucketer(2.0, 50.3, numBuckets)

    val count = Count.run(g.vertices)
    val op = VertexBucketGrid(xBucketer, yBucketer, 50000)
    val out = op(
      op.vertices, g.vertices)(
        op.filtered, g.vertices)(
          op.xAttribute, g.name)(
            op.yAttribute, g.age)(
              op.originalCount, count).result

    assert(out.buckets.value.counts == Map((0, 0) -> 1, (1, 0) -> 1, (2, 0) -> 1, (2, 1) -> 1))
    assert(out.xBuckets.rdd.collect.toMap == Map((0 -> 0), (1 -> 1), (2 -> 2), (3 -> 2)))
    assert(out.yBuckets.rdd.collect.toMap == Map((0 -> 0), (1 -> 0), (2 -> 1), (3 -> 0)))
  }

  test("Precise bucketing") {
    val numBuckets = 2
    val xBucketer = StringBucketer(Seq("Male", "Female"), hasOther = false)
    val yBucketer = DoubleLinearBucketer(2.0, 50.3, numBuckets)

    val count = Count.run(g.vertices)
    val op = VertexBucketGrid(xBucketer, yBucketer, -1)
    val out = op(
      op.vertices, g.vertices)(
        op.filtered, g.vertices)(
          op.xAttribute, g.gender)(
            op.yAttribute, g.age)(
              op.originalCount, count).result

    assert(out.buckets.value.counts == Map((1, 0) -> 1, (0, 1) -> 1, (0, 0) -> 2))
  }

  test("Non-precise bucketing") {
    val numBuckets = 2
    val xBucketer = StringBucketer(Seq("Male", "Female"), hasOther = false)
    val yBucketer = DoubleLinearBucketer(2.0, 50.3, numBuckets)

    val count = Count.run(g.vertices)
    val op = VertexBucketGrid(xBucketer, yBucketer, 2)
    val out = (op(op.vertices, g.vertices)(
      op.filtered, g.vertices)(
        op.xAttribute, g.gender)(
          op.yAttribute, g.age)(
            op.originalCount, count)).result

    for ((key, value) <- out.buckets.value.counts) {
      assert(value == 0) // All counters are zero because of rounding.
    }
  }
}
