// Package-level types and type aliases.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import play.api.libs.json

package object graph_api {
  type ID = Long

  type VertexSetRDD = UniqueSortedRDD[ID, Unit]

  type AttributeRDD[T] = UniqueSortedRDD[ID, T]

  case class Edge(src: ID, dst: ID) extends Ordered[Edge] with ToJson {
    def compare(other: Edge) =
      if (src != other.src) src.compare(other.src) else dst.compare(other.dst)
    override def toJson = Json.obj("src" -> src, "dst" -> dst)
  }

  object Edge extends FromJson[Edge] {
    def fromJson(j: json.JsValue) =
      Edge((j \ "src").as[ID], (j \ "dst").as[ID])
  }

  type EdgeBundleRDD = UniqueSortedRDD[ID, Edge]
}
