package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.util.UUID
import org.scalatest.FunSuite

import attributes.AttributeSignature

class BigGraphManagerTest extends FunSuite {
  val sysTempDir = System.getProperty("java.io.tmpdir")
  val myTempDir = new File("%s/bgmtest-%d".format(sysTempDir, scala.compat.Platform.currentTime))
  myTempDir.mkdir

  def tempDir(dirName: String) = new File(myTempDir, dirName)
  def cleanManager(dirName: String): BigGraphManager = {
    val managerDir = tempDir(dirName)
    managerDir.mkdir
    BigGraphManager(managerDir.toString)
  }

  test("We can derive a new graph from nothing and derive an other graph from it.") {
    val manager = cleanManager("fromnothing")
    val g1 = manager.deriveGraph(Seq(), FromNothing())
    val g2 = manager.deriveGraph(Seq(g1), FromAnother())
  }

  test("Retrieving by GUID give the same instance") {
    val manager = cleanManager("guidsameinstance")
    val g1 = manager.deriveGraph(Seq(), FromNothing())
    val g2 = manager.deriveGraph(Seq(g1), FromAnother())
    val g3 = manager.graphForGUID(g2.gUID).get
    assert(g2 eq g3)
  }

  test("Source list validity is checked.") {
    val manager = cleanManager("validity")
    val g1 = manager.deriveGraph(Seq(), FromNothing())
    intercept[AssertionError] {
      val g2 = manager.deriveGraph(Seq(g1), FromNothing())
    }
  }

  test("Sometimes, there is no such graph") {
    val manager = cleanManager("nosuchgraph")
    val g1 = manager.deriveGraph(Seq(), FromNothing())
    assert(manager.graphForGUID(UUID.randomUUID) == None)
  }

  test("Save and load works") {
    val d1 = tempDir("sld1")
    d1.mkdir
    val d2 = tempDir("sld2")
    d2.mkdir
    val m1o = BigGraphManager(d1.toString)
    val g1 = m1o.deriveGraph(Seq(), FromNothing())
    val m1c = BigGraphManager(d1.toString)
    // We have the graph for the UUID.
    val g1c = m1c.graphForGUID(g1.gUID).get

    // It is like the one we created.
    assert(g1c.sources.isEmpty)
    assert(g1c.operation.isInstanceOf[FromNothing])

    // It's not the same instance.
    assert(!(g1 eq g1c))

    // It didn't leak over to an unrelated manager.
    val m2o = BigGraphManager(d2.toString)
    assert(m2o.graphForGUID(g1.gUID) == None)
  }

  test("Derivatives work") {
    val manager = cleanManager("derivatives")
    val g1 = manager.deriveGraph(Seq(), FromNothing())
    val g2 = manager.deriveGraph(Seq(g1), FromAnother())
    val g3 = manager.deriveGraph(Seq(g1, g1), FromTwoOthers())
    val g4 = manager.deriveGraph(Seq(g1, g2), FromTwoOthers())
    val g5 = manager.deriveGraph(Seq(g2, g2), FromTwoOthers())

    val derivatives1 = manager.knownDirectDerivatives(g1).toSet
    assert(derivatives1.size == 3)
    assert(derivatives1.contains(g2))
    assert(derivatives1.contains(g3))
    assert(derivatives1.contains(g4))

    val derivatives2 = manager.knownDirectDerivatives(g2).toSet
    assert(derivatives2.size == 2)
    assert(derivatives2.contains(g4))
    assert(derivatives2.contains(g5))

  }

}

private case class FromNothing() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = sources.isEmpty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = ???

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty
}

private case class FromAnother() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = sources.size == 1

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = ???

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = sources.head.vertexAttributes

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = sources.head.edgeAttributes
}

private case class FromTwoOthers() extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]) = sources.size == 2

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = ???

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = sources.head.vertexAttributes

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = sources.head.edgeAttributes
}
