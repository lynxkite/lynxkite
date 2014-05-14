package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph._
import java.util.UUID
import scala.collection.mutable

import attributes.AttributeSignature

/**
 * BigGraph represents the lineage of a graph.
 *
 * Use gUID to uniquely identify a big graph. One should obtain
 * a BigGraph object either via the GraphManager using the gUID or
 * deriving it from (0 or more) existing BigGraphs via an operation.
 */
class BigGraph private[graph_api] (val sources: Seq[BigGraph], val operation: GraphOperation)
    extends Serializable {

  assert(operation.isSourceListValid(sources))

  lazy val gUID: UUID = {
    val collector = mutable.ArrayBuffer[Byte]()
    for (graph <- sources) {
      collector.appendAll(graph.gUID.toString.getBytes)
    }
    collector.appendAll(operation.gUID.toString.getBytes)
    UUID.nameUUIDFromBytes(collector.toArray)
  }

  @transient lazy val vertexAttributes: AttributeSignature =
      operation.vertexAttributes(sources)

  @transient lazy val edgeAttributes: AttributeSignature =
      operation.edgeAttributes(sources)

  @transient lazy val toLongString: String = "[%s](%s)".format(
      operation.toString,
      sources.map(_.toLongString).mkString(","))
}

/*
 * Interface for a repository of BigGraph objects.
 *
 * The methods in this class do not actually do any computation,
 * you need to use a GraphDataManager to obtain the actual data for a
 * BigGraph.
 */
abstract class BigGraphManager {
  // Creates a BigGraph object by applying the given operation
  // to the given source graphs.
  def deriveGraph(sources: Seq[BigGraph],
                  operation: GraphOperation): BigGraph

  // Returns the BigGraph corresponding to a given GUID.
  def graphForGUID(gUID: UUID): Option[BigGraph]

  // Returns all graphs in the meta graph known to this manager that has the given
  // graph as one of its sources.
  def knownDirectDerivatives(graph: BigGraph): Seq[BigGraph]

  def repositoryPath: String
}

object BigGraphManager {
  def apply(repositoryPath: String): BigGraphManager = {
    new BigGraphManagerImpl(repositoryPath)
  }

  // TODO: currently a hack for handling "x" initial request
  def getBigGraphForId(id: String, enviroment: BigGraphEnviroment): BigGraph = {
    if (id == "x") {
      enviroment.bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph2)
    } else {
      enviroment.bigGraphManager.graphForGUID(UUID.fromString(id)).get
    }
  }
}

// TODO: remove this once we have some more sane starting operations.
// For now this is a copy of the same class created in GraphTestUtils. The reason for the copy
// is that prod code cannot (or at least should not) depend on test code, but I don't want
// to move this out to some permanent non-test location as it will go away from prod code
// but will stick around in test code.
import graph_api._
import org.apache.spark.graphx.Edge
import attributes.AttributeSignature
import attributes.DenseAttributes

class InstantiateSimpleGraph2 extends GraphOperation {
  @transient var executionCounter = 0

  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val vertexSig = vertexAttributes(target.sources)
    val edgeSig = edgeAttributes(target.sources)

    val vertexMaker = vertexSig.maker
    val nameIdx = vertexSig.writeIndex[String]("name")
    val vertices = Seq(
        (0l, vertexMaker.make.set(nameIdx, "Adam")),
        (1l, vertexMaker.make.set(nameIdx, "Eve")),
        (2l, vertexMaker.make.set(nameIdx, "Bob")))

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

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("name").signature

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("comment").signature
}