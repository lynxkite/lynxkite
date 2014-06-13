package com.lynxanalytics.biggraph.graph_api

import java.io.File

import org.apache.spark
import org.apache.spark.graphx

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
  def cleanMetaGraphManager(dirName: String): MetaGraphManager = {
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
  def cleanDataManager(dirName: String): DataManager = {
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    new DataManager(sparkContext, Filename(managerDir.toString))
  }
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
  def signature = newSignature
    .outputGraph('vertices, 'edges)
    .outputVertexAttribute[String]('name, 'vertices)
    .outputVertexAttribute[Double]('age, 'vertices)
    .outputEdgeAttribute[String]('comment, 'edges)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
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
