package com.lynxanalytics.biggraph.graph_api

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

  @transient lazy val properties: BigGraphProperties =
    operation.targetProperties(sources)

  @transient lazy val toLongString: String = "[%s](%s)".format(
    operation.toString,
    sources.map(_.toLongString).mkString(","))
}

/*
 * Class used to characterize certain special classes of BigGraphs.
 *
 * If a property below is true, it means the graph is guaranteed to have that propery. False
 * does not mean that the graph is guaranteed to not have the property, it only means it is not
 * known.
 */
case class BigGraphProperties(
  // If there is an edge A->B then there is also an edge B->A.
  symmetricEdges: Boolean = false)

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
}
