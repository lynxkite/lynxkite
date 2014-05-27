package com.lynxanalytics.biggraph.graph_api

import java.io.File

import org.apache.spark
import org.apache.spark.graphx.Edge

import com.lynxanalytics.biggraph.TestTempDir
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.BigGraphEnviroment

import attributes.AttributeSignature
import attributes.DenseAttributes

trait TestBigGraphManager extends TestTempDir {
  def cleanGraphManager(dirName: String): BigGraphManager = {
    val managerDir = tempDir("graphManager." + dirName)
    managerDir.mkdir
    BigGraphManager(managerDir.toString)
  }
}

trait TestGraphDataManager extends TestTempDir with TestSparkContext {
  def cleanDataManager(dirName: String): GraphDataManager = {
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    GraphDataManager(sparkContext, managerDir.toString)
  }
}

class BigGraphTestEnviroment(dirName: String) extends BigGraphEnviroment with TestBigGraphManager with TestGraphDataManager {
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
      new Edge(0l, 1l, edgeMaker.make.set(commentIdx, "Adam loves Eve")),
      new Edge(1l, 0l, edgeMaker.make.set(commentIdx, "Eve loves Adam")),
      new Edge(2l, 0l, edgeMaker.make.set(commentIdx, "Bob envies Adam")),
      new Edge(2l, 1l, edgeMaker.make.set(commentIdx, "Bob loves Eve")))

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
