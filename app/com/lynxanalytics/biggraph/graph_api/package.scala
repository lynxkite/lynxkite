// Package-level types and type aliases.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.spark_util.SortedRDD

package object graph_api {
  type ID = Long

  type VertexSetRDD = SortedRDD[ID, Unit]

  type AttributeRDD[T] = SortedRDD[ID, T]

  case class Edge(val src: ID, val dst: ID) extends Ordered[Edge] {
    def compare(other: Edge) =
      if (src != other.src) src.compare(other.src) else dst.compare(other.dst)
  }

  type EdgeBundleRDD = SortedRDD[ID, Edge]
}
