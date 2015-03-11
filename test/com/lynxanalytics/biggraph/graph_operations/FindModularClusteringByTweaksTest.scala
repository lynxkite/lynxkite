package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.spark_util.Implicits._

class FindModularClusteringByTweaksTest extends FunSuite with ShouldMatchers with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val clusters = {
      val op = FindModularClusteringByTweaks()
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
      changeFromCall should be (realChange +- 1e-5)
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
      val op = FindModularClusteringByTweaks()
      op(op.edges, es)(op.weights, weights).result
    }
    val modularity = {
      val op = Modularity()
      op(op.edges, symmetricEs)(op.weights, symmetricWeights)(op.belongsTo, clusters.belongsTo)
        .result.modularity.value
    }
    assert(modularity > 0.45)
  }
}
