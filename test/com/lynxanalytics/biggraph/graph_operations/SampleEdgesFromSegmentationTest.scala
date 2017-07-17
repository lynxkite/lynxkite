package com.lynxanalytics.biggraph.graph_operations

import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.spark
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class SampleEdgesFromSegmentationTest extends FunSuite with TestGraphOp {

  def createColocationLikeSetup(numEvents: Int, numPersons: Int): (VertexSet, EdgeBundle) = {
    val vs1 = CreateVertexSet(numEvents).result.vs
    val timeAttr = vs1.randomAttribute(42)
    val personAttr = {
      val op = AddRandomAttribute(1, "Standard Uniform")
      op(op.vs, vs1).result.attr
    }
    val personIdAttr = personAttr.deriveX[String](s"(x * ${numPersons}).round.toString")
    val segmentation1 = {
      val op = DoubleBucketing(0.1, true)
      op(op.attr, timeAttr).result
    }
    val mergeResult = {
      val op = MergeVertices[String]()
      op(op.attr, personIdAttr).result
    }
    val vs2 = mergeResult.segments
    val segmentation2 = {
      val op = InducedEdgeBundle(induceDst = false)
      op(op.srcMapping, mergeResult.belongsTo)(
        op.edges, segmentation1.belongsTo).result.induced
    }
    // segmentation2.gUID
    (vs2, segmentation2)
  }

  test("sampleVertexPairs") {
    val sampler = SampleEdgesFromSegmentation(0.1, 20)

    val members = Seq[Long](1, 2000, 3, 4, 5)
    val res = sampler.sampleVertexPairs(members, 10, new JDKRandomGenerator())
    assert(res.size == 10)
    assert(res.distinct.size == res.size)
    for ((src, dst) <- res) {
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
      (6l, Edge(4, 101)),
      (7l, Edge(4, 102))
    )).sortUnique(p)
    val selectedEdges = sparkContext.parallelize(
      Seq((1l, 2l), (6l, 4l))).sort(p)
    val restrictedBelongsTo =
      sampler.getVertexToSegmentPairsForSampledEdges(
        sampler.getVsToSegDistinct(belongsTo),
        selectedEdges,
        p)
    assert(restrictedBelongsTo.collect.toSeq.sorted == Seq(
      (1, 101),
      (1, 102),
      (2, 101),
      // edge no. 4 (3, 101) is filtered out because vertex 3 is not part of selectedEdges
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
      (8l, Edge(3, 101)),
      (9l, Edge(4, 101)),
      (10l, Edge(4, 102))
    )).sortUnique(p)
    val selectedEdges = sparkContext.parallelize(
      Seq((1l, 2l), (1l, 2l), (2l, 1l), (3l, 4l))).sort(p)
    val edgeCounts = sampler.getEdgeMultiplicities(
      selectedEdges,
      sampler.getVsToSegDistinct(belongsTo),
      p).collect.toMap
    assert(edgeCounts == Map(
      (1, 2) -> 2,
      (2, 1) -> 2,
      (3, 4) -> 1
    ))
  }

  test("getEdgeMultiplicities - sum of edge multiplicities should be equal to the total distinct edge count") {
    val (vs, segmentationBelongsTo) = createColocationLikeSetup(200, 20)
    // Note: belongsTo can contain the same vertex -> edge pair multiple times.
    //
    val allPossibleEdges = {
      val op = EdgesFromSegmentation()
      op(
        op.belongsTo,
        segmentationBelongsTo
      ).result.es
    }
    val partitioner = allPossibleEdges.rdd.partitioner.get
    val allPossibleEdgesCount = segmentationBelongsTo.rdd
      .map { case (id, Edge(vertex, segment)) => (segment, vertex) }
      .groupByKey(partitioner)
      .values
      .map { members => { val size = members.toSeq.distinct.size; size * size } }
      .reduce { case (a, b) => a + b }

    val op = SampleEdgesFromSegmentation(0.5, 12345601)
    val edgesWithCounts = op.getEdgeMultiplicities(
      allPossibleEdges.rdd.values.map { e => e.src -> e.dst },
      op.getVsToSegDistinct(segmentationBelongsTo.rdd),
      partitioner
    )
    assert(allPossibleEdgesCount == edgesWithCounts.values.reduce { case (a, b) => a + b })
  }

  test("Sample size expected value") {
    val (vs, segmentationBelongsTo) = createColocationLikeSetup(1000, 100)
    val allEdges = {
      val op = EdgesFromSegmentation()
      op(op.belongsTo, segmentationBelongsTo).result.es
    }
    val numMergedEdges = allEdges
      .rdd
      .map { case (_, Edge(src, dst)) => (src, dst) -> (()) }
      .groupByKey
      .count

    val sampledEdges1 = {
      val op = SampleEdgesFromSegmentation(0.4, 123001)
      op(op.belongsTo, segmentationBelongsTo).result.es
    }
    val numSampledEdges1 = sampledEdges1.rdd.count

    val sampledEdges2 = {
      val op = SampleEdgesFromSegmentation(0.04, 123002)
      op(op.belongsTo, segmentationBelongsTo).result.es
    }
    val numSampledEdges2 = sampledEdges2.rdd.count

    // The expected value of numSampledEdges1 should be 0.4 * numMergedEdges
    assert(numMergedEdges * 0.37 <= numSampledEdges1 && numSampledEdges1 <= numMergedEdges * 0.43)
    assert(numMergedEdges * 0.037 <= numSampledEdges2 && numSampledEdges2 <= numMergedEdges * 0.043)

  }

}
