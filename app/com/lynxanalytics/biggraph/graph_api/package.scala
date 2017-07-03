// Package-level types and type aliases.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

package object graph_api {
  type ID = Long

  type VertexSetRDD = UniqueSortedRDD[ID, Unit]

  type AttributeRDD[T] = UniqueSortedRDD[ID, T]

  type EdgeBundleRDD = UniqueSortedRDD[ID, Edge]

  type HybridBundleRDD = HybridRDD[ID, ID]
}

package graph_api {
  case class Edge(src: ID, dst: ID) extends Ordered[Edge] {
    def compare(other: Edge) =
      if (src != other.src) src.compare(other.src) else dst.compare(other.dst)
  }
}
