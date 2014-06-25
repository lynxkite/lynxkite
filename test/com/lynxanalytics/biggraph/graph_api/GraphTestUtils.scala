package com.lynxanalytics.biggraph.graph_api

import java.io.File
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx
import scala.util.Random

import com.lynxanalytics.biggraph.TestTempDir
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.BigGraphEnvironment

import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.RDDUtils

import attributes.AttributeSignature
import attributes.DenseAttributes

trait TestBigGraphManager extends TestTempDir {
  def cleanGraphManager(dirName: String): BigGraphManager = {
    val managerDir = tempDir("graphManager." + dirName)
    managerDir.mkdir
    BigGraphManager(managerDir.toString)
  }
}

trait TestMetaGraphManager extends TestTempDir {
  def cleanMetaManager: MetaGraphManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("metaGraphManager." + dirName)
    managerDir.mkdir
    new MetaGraphManager(managerDir.toString)
  }
}

trait TestGraphDataManager extends TestTempDir with TestSparkContext {
  def cleanGraphDataManager(dirName: String): GraphDataManager = {
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    GraphDataManager(sparkContext, Filename(managerDir.toString))
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
  def localData[T](vertexAttribute: VertexAttribute[T]): Map[Long, T] =
    rdd(vertexAttribute).collect.toMap

  def rdd[T](edgeAttribute: EdgeAttribute[T]): AttributeRDD[T] =
    dataManager.get(edgeAttribute).rdd
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

class BigGraphTestEnvironment(dirName: String)
    extends BigGraphEnvironment with TestBigGraphManager with TestGraphDataManager with TestGraphOperation {
  lazy val bigGraphManager = cleanGraphManager(dirName)
  lazy val graphDataManager = cleanGraphDataManager(dirName)
  lazy val metaGraphManager = cleanMetaManager
  lazy val dataManager = cleanDataManager
}

class InstantiateSimpleGraph extends GraphOperation {
  @transient var executionCounter = 0

  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val vertexSig = vertexAttributes(target.sources)
    val edgeSig = edgeAttributes(target.sources)

    val vertexMaker = vertexSig.maker
    val nameIdx = vertexSig.writeIndex[String]("name")
    val ageIdx = vertexSig.writeIndex[Double]("age")
    val vertices = Seq(
      (0l, vertexMaker.make.set(nameIdx, "Adam").set(ageIdx, 20.3)),
      (1l, vertexMaker.make.set(nameIdx, "Eve").set(ageIdx, 18.2)),
      (2l, vertexMaker.make.set(nameIdx, "Bob").set(ageIdx, 50.3)))

    val edgeMaker = edgeSig.maker
    val commentIdx = edgeSig.writeIndex[String]("comment")
    val edges = Seq(
      new graphx.Edge(0l, 1l, edgeMaker.make.set(commentIdx, "Adam loves Eve")),
      new graphx.Edge(1l, 0l, edgeMaker.make.set(commentIdx, "Eve loves Adam")),
      new graphx.Edge(2l, 0l, edgeMaker.make.set(commentIdx, "Bob envies Adam")),
      new graphx.Edge(2l, 1l, edgeMaker.make.set(commentIdx, "Bob loves Eve")))

    executionCounter += 1

    return new SimpleGraphData(target, sc.parallelize(vertices), sc.parallelize(edges))
  }

  @transient private lazy val internalVertexAttributes =
    AttributeSignature.empty.addAttribute[String]("name").addAttribute[Double]("age").signature
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = internalVertexAttributes

  @transient private lazy val internalEdgeAttributes =
    AttributeSignature.empty.addAttribute[String]("comment").signature
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = internalEdgeAttributes
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
