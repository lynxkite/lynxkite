package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.collection.mutable

import attributes.DenseAttributes
import attributes.AttributeSignature

// TODO(xandrew)
abstract class GraphOperation {
  def gUID: UUID
  def areSourcesCompatible(sources: Seq[BigGraph]): Boolean
  def vertexProperties(sources: Seq[BigGraph]): AttributeSignature
  def edgeProperties(sources: Seq[BigGraph]): AttributeSignature
}

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

/**
 * Generic representation of a graph as RDDs.
 *
 * Different implementations might have different performance tradeoffs.
 * Use GraphDataManager to get the data for a BigGraph.
 */
trait GraphData {
  type VertexRDD = rdd.RDD[(graphx.VertexId, DenseAttributes)]
  type EdgeRDD = rdd.RDD[graphx.Edge[DenseAttributes]]
  type TripletRDD = rdd.RDD[graphx.EdgeTriplet[DenseAttributes, DenseAttributes]]

  val bigGraph: BigGraph

  def vertices: VertexRDD
  def edges: EdgeRDD
  def triplets: TripletRDD

  def persist(level: spark.storage.StorageLevel)
  def unPersist()
}
