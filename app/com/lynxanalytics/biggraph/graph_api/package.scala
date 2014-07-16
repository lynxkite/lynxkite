package com.lynxanalytics.biggraph

import org.apache.spark.rdd

package object graph_api {
  type ID = Long

  type VertexSetRDD = rdd.RDD[(ID, Unit)]

  type AttributeRDD[T] = rdd.RDD[(ID, T)]

  case class Edge(val src: ID, val dst: ID)
  type EdgeBundleRDD = rdd.RDD[(ID, Edge)]
}
