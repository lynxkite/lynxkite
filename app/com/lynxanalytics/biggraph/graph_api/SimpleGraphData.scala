package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx
import org.apache.spark.rdd
import org.apache.spark.storage.StorageLevel

import attributes.DenseAttributes

class SimpleGraphData(val bigGraph: BigGraph,
                      val vertices: VertexRDD,
                      val edges: EdgeRDD,
                      userProvidedTriplets: TripletRDD = null)
    extends GraphData {
  val triplets: TripletRDD =
    if (userProvidedTriplets != null) {
      userProvidedTriplets
    } else {
      edges
        .map(edge => (edge.srcId, edge))
        .join(vertices)
        .map({
          case (srcId, (edge, srcAttr)) => {
            val triplet = new graphx.EdgeTriplet[DenseAttributes, DenseAttributes]()
            triplet.srcId = srcId
            triplet.dstId = edge.dstId
            triplet.srcAttr = srcAttr
            triplet.attr = edge.attr
            (edge.dstId, triplet)
          }
        })
        .join(vertices)
        .map({
          case (dstId, (triplet, dstAttr)) => {
            triplet.dstAttr = dstAttr
            triplet
          }
        })
    }

  cache()

  // Set RDD names.
  private val namePrefix = "Graph %s".format(bigGraph.gUID)
  vertices.name = namePrefix + " Vertices"
  edges.name = namePrefix + " Edges"
  triplets.name = namePrefix + " Triplets"

  lazy val numVertices = vertices.count
  lazy val numEdges = edges.count

  private def cacheIfUncached(rddToCache: rdd.RDD[_]): Unit =
    if (rddToCache.getStorageLevel == StorageLevel.NONE) rddToCache.cache()

  private def cache(): Unit = {
    cacheIfUncached(vertices)
    cacheIfUncached(edges)
    cacheIfUncached(triplets)
  }
}
