package com.lynxanalytics.biggraph.graph_api

import org.apache.spark.SparkContext._
import org.apache.spark.graphx
import org.apache.spark.graphx.VertexId
import scala.collection.immutable
import scala.collection.mutable
import scala.util.Sorting

object CompactUndirectedGraph {
  def apply(edges: EdgeBundleData): CompactUndirectedGraph = {
    val outEdges = edges.rdd.map {
      case (id, edge) => (edge.src, edge.dst)
    }.groupByKey()
    val inEdges = edges.rdd.map {
      case (id, edge) => (edge.dst, edge.src)
    }.groupByKey()
    val adjList = outEdges.join(inEdges)
      .map({
        case (v, (outs, ins)) => (v, (outs.toSet & ins.toSet - v).toArray.sorted)
      })
    val compact = adjList.mapPartitions({
      it: Iterator[(ID, Array[ID])] =>
        {
          val neighbors = mutable.ArrayBuffer[VertexId]()
          val indices = mutable.ArrayBuffer[Int]()
          val ids = mutable.ArrayBuffer[VertexId]()
          var index = 0
          it.foreach {
            case (v, ns) => {
              ids += v
              indices += index
              neighbors ++= ns
              index += ns.size
            }
          }
          Iterator(((ids.toArray, indices.toArray), neighbors.toArray))
        }
    })

    val perPartitionData = compact.collect
    val numVertices = perPartitionData
      .map { case ((ids, indices), neighbors) => ids.size }
      .sum
    val numEdges = perPartitionData
      .map { case ((ids, indices), neighbors) => neighbors.size }
      .sum

    val fullNeighbors = Array.ofDim[VertexId](numEdges)
    val vertexIndices = Array.ofDim[(VertexId, Int)](numVertices)
    val starts = Array.ofDim[Int](numVertices + 1)

    var offset = 0
    var index = 0
    perPartitionData.foreach({
      case ((ids, indices), neighbors) => {
        Array.copy(neighbors, 0, fullNeighbors, offset, neighbors.size)
        for (i <- 0 until ids.size) {
          starts(index + i) = indices(i) + offset
          vertexIndices(index + i) = (ids(i), index + i)
        }
        offset += neighbors.size
        index += ids.size
      }
    })
    starts(index) = offset

    Sorting.quickSort(vertexIndices)

    return new CompactUndirectedGraph(
      fullNeighbors, vertexIndices, starts)
  }
}

class CompactUndirectedGraph(
    fullNeighbors: Array[ID],
    indices: Array[(ID, Int)],
    starts: Array[Int]) extends Serializable {

  def findId(id: ID): Int = {
    var lb = 0
    var ub = indices.size

    while (lb < ub) {
      val mid = (lb + ub) / 2
      val (currId, currIndex) = indices(mid)
      if (currId == id) {
        return currIndex
      } else if (currId > id) {
        ub = mid
      } else {
        lb = mid + 1
      }
    }
    return -1
  }

  def getNeighbors(vid: ID): Seq[VertexId] = {
    val idx = findId(vid)
    if (idx == -1) {
      Seq()
    } else {
      fullNeighbors.view.slice(starts(idx), starts(idx + 1))
    }
  }
}
