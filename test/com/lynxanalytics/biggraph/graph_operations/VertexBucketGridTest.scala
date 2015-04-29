package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util._

class VertexBucketGridTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("Only 1 bucket") {
    val xBucketer = new EmptyBucketer()
    val yBucketer = new EmptyBucketer()
    val count = CountVertices.run(g.vertices)
    val op = VertexBucketGrid[Nothing, Nothing](xBucketer, yBucketer)
    val out = op(op.vertices, g.vertices)(op.filtered, g.vertices)(op.originalCount, count).result
    assert(out.buckets.value.counts == Map((0, 0) -> 4))
  }

  test("String-Double bucketing") {
    val numBuckets = 2
    val xBucketer = StringBucketer(Seq("Adam", "Eve"), hasOther = true)
    val yBucketer = DoubleLinearBucketer(2.0, 50.3, numBuckets)

    val cop = CountVertices()
    val count = cop(cop.vertices, g.vertices).result.count
    val op = VertexBucketGrid(xBucketer, yBucketer)
    val out = op(op.vertices, g.vertices)(op.filtered, g.vertices)(op.xAttribute, g.name)(op.yAttribute, g.age)(op.originalCount, count).result

    assert(out.buckets.value.counts == Map((0, 0) -> 1, (1, 0) -> 1, (2, 0) -> 1, (2, 1) -> 1))
    assert(out.xBuckets.rdd.collect.toMap == Map((0 -> 0), (1 -> 1), (2 -> 2), (3 -> 2)))
    assert(out.yBuckets.rdd.collect.toMap == Map((0 -> 0), (1 -> 0), (2 -> 1), (3 -> 0)))
  }
}
