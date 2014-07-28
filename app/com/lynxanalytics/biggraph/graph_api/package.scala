package com.lynxanalytics.biggraph

import org.apache.spark.rdd

import com.lynxanalytics.biggraph.spark_util.SortedRDD

package object graph_api {
  type ID = Long

  type VertexSetRDD = SortedRDD[ID, Unit]

  type AttributeRDD[T] = SortedRDD[ID, T]

  case class Edge(val src: ID, val dst: ID)
  type EdgeBundleRDD = SortedRDD[ID, Edge]
}
