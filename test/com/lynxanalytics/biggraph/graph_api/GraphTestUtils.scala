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
  def cleanDataManager(dirName: String): GraphDataManager = {
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

  def smallGraph(edgeLists: Map[Int, Seq[Int]]): (VertexSet, EdgeBundle) = {
    val outs = apply(SmallTestGraph(edgeLists))
    (outs.vertexSets('vs), outs.edgeBundles('es))
  }

  def localData(vertexSet: VertexSet): Set[Long] = {
    dataManager.get(vertexSet).rdd.keys.collect.toSet
  }
  def localData(edgeBundle: EdgeBundle): Set[(Long, Long)] = {
    dataManager
      .get(edgeBundle)
      .rdd
      .collect
      .map { case (id, edge) => (edge.src, edge.dst) }
      .toSet
  }
  def localData[T](vertexAttribute: VertexAttribute[T]): Map[Long, T] = {
    dataManager.get(vertexAttribute).rdd.collect.toMap
  }
  def localData[T](edgeAttribute: EdgeAttribute[T]): Map[(Long, Long), T] = {
    val edgesRDD = dataManager.get(edgeAttribute.edgeBundle).rdd
    val attrRDD = dataManager.get(edgeAttribute).rdd
    edgesRDD.join(attrRDD).map {
      case (id, (edge, value)) =>
        (edge.src, edge.dst) -> value
    }.collect.toMap
  }
}

trait TestGraphOperation extends TestMetaGraphManager with TestDataManager {
  def cleanHelper = new GraphOperationTestHelper(cleanMetaManager, cleanDataManager)
}

class BigGraphTestEnvironment(dirName: String) extends BigGraphEnvironment with TestBigGraphManager with TestGraphDataManager {
  lazy val bigGraphManager = cleanGraphManager(dirName)
  lazy val graphDataManager = cleanDataManager(dirName)
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

case class CreateExampleGraphOperation() extends MetaGraphOperation {
  @transient var executionCounter = 0

  def signature = newSignature
    .outputGraph('vertices, 'edges)
    .outputVertexAttribute[String]('name, 'vertices)
    .outputVertexAttribute[Double]('age, 'vertices)
    .outputEdgeAttribute[String]('comment, 'edges)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    outputs.putVertexSet('vertices, sc.parallelize(Seq(0l, 1l, 2l).map((_, ()))))
    outputs.putEdgeBundle('edges, sc.parallelize(Seq(
      (0l, Edge(0l, 1l)),
      (1l, Edge(1l, 0l)),
      (2l, Edge(2l, 0l)),
      (3l, Edge(2l, 1l)))))
    outputs.putVertexAttribute[String]('name, sc.parallelize(Seq(
      (0l, "Adam"),
      (1l, "Eve"),
      (2l, "Bob"))))
    outputs.putVertexAttribute[Double]('age, sc.parallelize(Seq(
      (0l, 20.3),
      (1l, 18.2),
      (2l, 50.3))))
    outputs.putEdgeAttribute[String]('comment, sc.parallelize(Seq(
      (0l, "Adam loves Eve"),
      (1l, "Eve loves Adam"),
      (2l, "Bob envies Adam"),
      (3l, "Bob loves Eve"))))
  }
}

case class SmallTestGraph(edgeLists: Map[Int, Seq[Int]]) extends MetaGraphOperation {
  def signature = newSignature.outputGraph('vs, 'es)
  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    outputs.putVertexSet('vs, sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ()))))
    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    outputs.putEdgeBundle('es, sc.parallelize(nodePairs.zipWithIndex.map {
      case ((a, b), i) => i.toLong -> Edge(a, b)
    }))
  }
}
