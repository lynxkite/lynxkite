package com.lynxanalytics.biggraph

import org.apache.spark.graphx
import org.apache.spark.rdd

import graph_api.attributes.DenseAttributes

package object graph_api {

  /*
   * RDD type for representing graph vertices.
   */
  type VertexRDD = rdd.RDD[(graphx.VertexId, DenseAttributes)]

  /*
   * RDD type for representing graph edges.
   */
  type EdgeRDD = rdd.RDD[graphx.Edge[DenseAttributes]]

  /*
   * RDD type for representing graph triplets. (A triplet is an edge together with its endpoints.)
   */
  type TripletRDD = rdd.RDD[graphx.EdgeTriplet[DenseAttributes, DenseAttributes]]

  type ID = Long

  type VertexSetRDD = rdd.RDD[(ID, Unit)]

  type AttributeRDD[T] = rdd.RDD[(ID, T)]

  case class Edge(val src: ID, val dst: ID)
  type EdgeBundleRDD = rdd.RDD[(ID, Edge)]
}
