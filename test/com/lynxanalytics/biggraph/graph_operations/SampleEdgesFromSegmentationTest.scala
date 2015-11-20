package com.lynxanalytics.biggraph.graph_operations

import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.spark
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class SampleEdgesFromSegmentationTest extends FunSuite with TestGraphOp {

  def createGraphWithOverlappingSegments(numVertices: Int) = {
    val vs = CreateVertexSet(numVertices).result.vs
    val attr1 = {
      val op = AddGaussianVertexAttribute(42)
      op(op.vertices, vs).result.attr
    }
    val attr2 = {
      val op = AddGaussianVertexAttribute(43)
      op(op.vertices, vs).result.attr
    }
    val attr3 = {
      DeriveJS.deriveFromAttributes[Double](
        "attr1 + Math.abs(attr2) / 10",
        Seq("attr1" -> attr1, "attr2" -> attr2),
        vs).attr
    }
    val segmentation = {
      val op = IntervalBucketing(0.1, true)
      op(op.beginAttr, attr1)(op.endAttr, attr3).result
    }
    (vs, segmentation)
  }

  test("sampleEdges") {
    val sampler = SampleEdgesFromSegmentation(0.1, 20)

    val members = Seq[Long](1, 2000, 3, 4, 5)
    val res = sampler.sampleVertexPairs(members, 10, new JDKRandomGenerator())
    assert(res.size == 10)
    assert(res.distinct.size == res.size)
    for (Edge(src, dst) <- res) {
      assert(members.contains(src))
      assert(members.contains(dst))
    }
  }

  test("getVertexToSegmentPairsForSampledEdges") {
    val p = new spark.HashPartitioner(2)

    val sampler = SampleEdgesFromSegmentation(0.1, 20)
    val belongsTo = sparkContext.parallelize(Seq(
      (1l, Edge(1, 101)),
      (2l, Edge(1, 102)),
      (3l, Edge(2, 101)),
      (4l, Edge(3, 101)),
      (5l, Edge(4, 101)),
      (6l, Edge(4, 102))
    )).sortUnique(p)
    val selectedEdges = sparkContext.parallelize(
      Seq((1l, 2l), (6l, 4l))).sort(p)
    val restrictedBelongsTo = sampler.getVertexToSegmentPairsForSampledEdges(belongsTo, selectedEdges, p)
    assert(restrictedBelongsTo.collect.toSeq.sorted == Seq(
      (1, 101),
      (1, 102),
      (2, 101),
      // edge no. 5 (4, 101) is filtered out
      (4, 101),
      (4, 102)
    ))
  }

  test("getEdgeMultiplicities") {
    val p = new spark.HashPartitioner(2)

    val sampler = SampleEdgesFromSegmentation(0.1, 20)
    val belongsTo = sparkContext.parallelize(Seq(
      (1l, Edge(1, 101)),
      (2l, Edge(1, 102)),
      (3l, Edge(1, 103)),
      (4l, Edge(2, 101)),
      (5l, Edge(2, 103)),
      (6l, Edge(2, 105)),
      (7l, Edge(3, 101)),
      (8l, Edge(4, 101)),
      (9l, Edge(4, 102))
    )).sortUnique(p)
    val selectedEdges = sparkContext.parallelize(
      Seq((1l, 2l), (2l, 1l), (3l, 4l))).sort(p)
    val edgeCounts = sampler.getEdgeMultiplicities(selectedEdges, belongsTo, p).collect.toMap
    assert(edgeCounts == Map(
      Edge(1, 2) -> 2,
      Edge(2, 1) -> 2,
      Edge(3, 4) -> 1
    ))
  }

  test("getEdgeMultiplicities: sum of edge multiplicities should be equal to the total edge count") {
    val (vs, segmentation) = createGraphWithOverlappingSegments(200)
    val allPossibleEdges = {
      val op = EdgesFromSegmentation()
      op(op.belongsTo, segmentation.belongsTo).result.es
    }
    val op = SampleEdgesFromSegmentation(0.5, 12345601)
    val edgesWithCounts = op.getEdgeMultiplicities(
      allPossibleEdges.rdd.values.map { e => e.src -> e.dst }.sort(allPossibleEdges.rdd.partitioner.get),
      segmentation.belongsTo.rdd,
      allPossibleEdges.rdd.partitioner.get
    )
    assert(allPossibleEdges.count.get == edgesWithCounts.values.reduce { case (a, b) => a + b })
  }

  test("Sample size expected value") {
    val (vs, segmentation) = createGraphWithOverlappingSegments(200)
    val allEdges = {
      val op = EdgesFromSegmentation()
      op(op.belongsTo, segmentation.belongsTo).result.es
    }
    val numMergedEdges = allEdges
      .rdd
      .map { case (_, Edge(src, dst)) => (src, dst) -> () }
      .groupByKey
      .count
    val sampledEdges = {
      val op = SampleEdgesFromSegmentation(0.4, 123456)
      op(op.belongsTo, segmentation.belongsTo).result.es
    }
    val numSampledEdges = sampledEdges.rdd.count
    // The expected value of numSampledEdges should be 0.4 * numMergedEdges
    assert(numMergedEdges * 0.37 <= numSampledEdges && numSampledEdges <= numMergedEdges * 0.43)
  }
}
