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
class BigGraph private[graph_api] (val sources: Seq[BigGraph],
                                   val operation: GraphOperation) {

  assert(operation.areSourcesCompatible(sources))

  private lazy val gUID: UUID = {
    val collector = mutable.ArrayBuffer[Byte]()
    for (graph <- sources) {
      collector.appendAll(graph.gUID.toString.getBytes)
    }
    collector.appendAll(operation.gUID.toString.getBytes)
    UUID.nameUUIDFromBytes(collector.toArray)
  }

  lazy val vertexProperties: AttributeSignature =
      operation.vertexProperties(sources)

  lazy val edgeProperties: AttributeSignature =
      operation.edgeProperties(sources)
}

/*
 * Interface for a repository of BigGraph objects.
 */
abstract class BigGraphManager {
  def deriveGraph(sources: Seq[BigGraph],
                  operation: GraphOperation): BigGraph

  def graphForGUID(gUID: UUID): BigGraph

  def knownDerivates(graph: BigGraph): Seq[BigGraph]
}
