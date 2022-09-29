package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import scala.util.Random

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class FindModularClusteringByTweaksTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val clusters = {
      val op = FindModularClusteringByTweaks(-1, 0.001)
      op(op.edges, eg.edges)(op.weights, eg.weight).result
    }
    val clusterMap = clusters.belongsTo.toPairSeq.toMap
    assert(clusterMap.values.toSet.size == 2)
    assert(clusterMap(0) == clusterMap(1))
    assert(clusterMap(0) == clusterMap(2))
  }
  test("cluster addition test") {
    import FindModularClusteringByTweaks._
    val rnd = new Random(0)
    for (i <- 0 until 30) {
      val connection = rnd.nextDouble() * 50
      val c1Inside = rnd.nextDouble() * 100
      val c1All = rnd.nextDouble() max (c1Inside + connection)
      val c1 = ClusterData(c1All, c1Inside, rnd.nextInt(100))
      val c2Inside = rnd.nextDouble() * 100
      val c2All = rnd.nextDouble() max (c2Inside + connection)
      val c2 = ClusterData(c2All, c2Inside, rnd.nextInt(100))
      val total = (rnd.nextDouble() * 1000) max (c1All + c2All)
      val changeFromCall = mergeModularityChange(total, c1, c2, connection)
      val realChange =
        c1.add(connection, c2).modularity(total) - c1.modularity(total) - c2.modularity(total)
      changeFromCall should be(realChange +- 1e-5)
    }
  }
  test("random graph") {
    val vs = CreateVertexSet(100)().result.vs
    val es = {
      val op = FastRandomEdgeBundle(0, 2.3)
      op(op.vs, vs).result.es
    }
    val weights = AddConstantAttribute.run(es.idSet, 1.0)
    val symmetricEs = {
      val op = AddReversedEdges()
      op(op.es, es).result.esPlus
    }
    val symmetricWeights = AddConstantAttribute.run(symmetricEs.idSet, 1.0)
    val clusters = {
      val op = FindModularClusteringByTweaks(-1, 0.001)
      op(op.edges, es)(op.weights, weights).result
    }
    val modularity = {
      val op = Modularity()
      op(op.edges, symmetricEs)(op.weights, symmetricWeights)(op.belongsTo, clusters.belongsTo)
        .result.modularity.value
    }
    assert(modularity > 0.45)
  }

  test("cluster spectrum computes modularity change correctly") {
    import FindModularClusteringByTweaks._
    val rnd = new Random(0)
    val edgeLists = (0 until 20)
      .map(_ => (rnd.nextInt(10).toLong, rnd.nextInt(10).toLong, rnd.nextDouble() * 10))
      .flatMap { case (from, to, weight) => Iterator((from, to, weight), (to, from, weight)) }
      .groupBy(_._1)
      .mapValues(friends => friends.groupBy(_._2).mapValues(weights => weights.map(_._3).sum).toSeq)

    val degrees = edgeLists.mapValues(edges => edges.map(_._2).sum)
    val totalDegreeSum = degrees.map(_._2).sum
    val fullClusterMembers = Set(0L, 1L, 2L, 3L, 4L)
    val fullCluster = ClusterData.fromMembers(fullClusterMembers, edgeLists)
    val part1Members = Set(0L, 2L, 4L)
    val part1 = ClusterData.fromMembers(part1Members, edgeLists)
    val part2 = ClusterData.fromMembers(fullClusterMembers -- part1Members, edgeLists)
    val realModularityChange =
      part1.modularity(totalDegreeSum) + part2.modularity(totalDegreeSum) -
        fullCluster.modularity(totalDegreeSum)
    val spectrum = new ClusterSpectrum(
      totalDegreeSum,
      fullCluster,
      fullClusterMembers,
      degrees,
      edgeLists)

    val plusMinusOneVector = spectrum.toPlusMinus(part1Members)
    val estimatedModularityChange =
      spectrum.dot(plusMinusOneVector, spectrum.multiply(plusMinusOneVector)) / totalDegreeSum / 2.0

    estimatedModularityChange should be(realModularityChange +- 1e-7)
  }

  test("cluster spectrum cuts two connected triangles") {
    import FindModularClusteringByTweaks._
    val edgeLists = Map(
      // Two triangles connected by a single edge.
      0L -> Seq((1L, 1.0), (2L, 1.0)),
      1L -> Seq((0L, 1.0), (2L, 1.0)),
      2L -> Seq((0L, 1.0), (1L, 1.0), (3L, 1.0)),
      3L -> Seq((2L, 1.0), (4L, 1.0), (5L, 1.0)),
      4L -> Seq((3L, 1.0), (5L, 1.0)),
      5L -> Seq((3L, 1.0), (4L, 1.0)),
      // Plus one more dummy point.
      6L -> Seq((1L, 1.0), (3L, 1.0)),
    )

    val degrees = edgeLists.mapValues(edges => edges.map(_._2).sum)
    val totalDegreeSum = degrees.map(_._2).sum
    val fullClusterMembers = Set(0L, 1L, 2L, 3L, 4L, 5L)
    val fullCluster = ClusterData.fromMembers(fullClusterMembers, edgeLists)
    val spectrum = new ClusterSpectrum(
      totalDegreeSum,
      fullCluster,
      fullClusterMembers,
      degrees,
      edgeLists)
    val rnd = new Random(0)
    val (clust1, clust2, _) = spectrum.bestSplit(rnd)
    val clustWith0 = if (clust1.contains(0L)) clust1 else clust2
    assert(clust1 == Set(0L, 1L, 2L))
  }
}
