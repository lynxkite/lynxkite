package com.lynxanalytics.biggraph.graph_api

import java.io.File
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.util.Random

import com.lynxanalytics.biggraph.TestTempDir
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.BigGraphEnvironment

import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.RDDUtils

trait TestMetaGraphManager extends TestTempDir {
  def cleanMetaManager: MetaGraphManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("metaGraphManager." + dirName)
    managerDir.mkdir
    new MetaGraphManager(managerDir.toString)
  }
}

trait TestDataManager extends TestTempDir with TestSparkContext {
  def cleanDataManager: DataManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    new DataManager(sparkContext, Filename(managerDir.toString))
  }
}

class GraphOperationTestHelper(val metaManager: MetaGraphManager,
                               val dataManager: DataManager) {
  def apply(operation: MetaGraphOperation,
            inputs: MetaDataSet = MetaDataSet()): MetaDataSet = {
    metaManager.apply(operation, inputs).outputs
  }

  def apply(operation: MetaGraphOperation,
            all: Map[Symbol, MetaGraphEntity]): MetaDataSet = {
    apply(operation, MetaDataSet(all))
  }

  def apply(operation: MetaGraphOperation,
            all: (Symbol, MetaGraphEntity)*): MetaDataSet = {
    metaManager.apply(operation, all: _*).outputs
  }

  def smallGraph(edgeLists: Map[Int, Seq[Int]]): (VertexSet, EdgeBundle) = {
    val outs = apply(SmallTestGraph(edgeLists))
    (outs.vertexSets('vs), outs.edgeBundles('es))
  }

  def groupedGraph(edgeLists: Seq[(Seq[Int], Int)]): (VertexSet, VertexSet, EdgeBundle, EdgeAttribute[Double]) = {
    val (srcs, dsts) = edgeLists.unzip
    val (vs, _) = smallGraph(srcs.flatten.map(_ -> Seq()).toMap)
    val (sets, _) = smallGraph(dsts.map(_ -> Seq()).toMap)
    val es = edgeLists.flatMap { case (s, i) => s.map(_.toLong -> i.toLong) }
    val edges = apply(AddWeightedEdges(es, 1.0), 'src -> vs, 'dst -> sets)
    (vs, sets, edges.edgeBundles('es), edges.edgeAttributes('weight).runtimeSafeCast[Double])
  }

  def rdd(vertexSet: VertexSet): VertexSetRDD = dataManager.get(vertexSet).rdd
  def localData(vertexSet: VertexSet): Set[Long] = rdd(vertexSet).keys.collect.toSet

  def rdd(edgeBundle: EdgeBundle): EdgeBundleRDD = dataManager.get(edgeBundle).rdd
  def localData(edgeBundle: EdgeBundle): Set[(Long, Long)] = {
    rdd(edgeBundle)
      .collect
      .map { case (id, edge) => (edge.src, edge.dst) }
      .toSet
  }

  def rdd[T](vertexAttribute: VertexAttribute[T]): AttributeRDD[T] =
    dataManager.get(vertexAttribute).rdd
  def localData[T: reflect.runtime.universe.TypeTag](vertexAttribute: VertexAttribute[_]): Map[Long, T] =
    localData(vertexAttribute.runtimeSafeCast[T])
  def localData[T](vertexAttribute: VertexAttribute[T]): Map[Long, T] =
    rdd(vertexAttribute).collect.toMap

  def rdd[T](edgeAttribute: EdgeAttribute[T]): AttributeRDD[T] =
    dataManager.get(edgeAttribute).rdd
  def localData[T: reflect.runtime.universe.TypeTag](edgeAttribute: EdgeAttribute[_]): Map[Long, T] =
    localData(edgeAttribute.runtimeSafeCast[T])
  def localData[T](edgeAttribute: EdgeAttribute[T]): Map[(Long, Long), T] = {
    val edgesRDD = rdd(edgeAttribute.edgeBundle)
    val attrRDD = rdd(edgeAttribute)
    edgesRDD.join(attrRDD).map {
      case (id, (edge, value)) =>
        (edge.src, edge.dst) -> value
    }.collect.toMap
  }

  def localData[T](scalar: Scalar[T]): T = dataManager.get(scalar).value
}

object HelperSingletonProvider extends TestMetaGraphManager with TestDataManager {
  lazy val helper = new GraphOperationTestHelper(cleanMetaManager, cleanDataManager)
}

trait TestGraphOperation extends TestMetaGraphManager with TestDataManager {
  val helper = HelperSingletonProvider.helper
  def cleanHelper = new GraphOperationTestHelper(cleanMetaManager, cleanDataManager)
}

case class SmallTestGraph(edgeLists: Map[Int, Seq[Int]]) extends MetaGraphOperation {
  def signature = newSignature.outputGraph('vs, 'es)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    outputs.putVertexSet(
      'vs,
      sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ())))
        .partitionBy(rc.onePartitionPartitioner))

    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    outputs.putEdgeBundle(
      'es,
      sc.parallelize(nodePairs.zipWithIndex.map {
        case ((a, b), i) => i.toLong -> Edge(a, b)
      })
        .partitionBy(rc.onePartitionPartitioner))
  }
}

case class AddWeightedEdges(edges: Seq[(ID, ID)], weight: Double) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('src)
    .inputVertexSet('dst)
    .outputEdgeBundle('es, 'src -> 'dst)
    .outputEdgeAttribute[Double]('weight, 'es)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val es = RDDUtils.fastNumbered(rc.sparkContext.parallelize(edges.map {
      case (a, b) => Edge(a, b)
    })).partitionBy(rc.onePartitionPartitioner)
    outputs.putEdgeBundle('es, es)
    outputs.putEdgeAttribute('weight, es.mapValues(_ => weight))
  }
}
