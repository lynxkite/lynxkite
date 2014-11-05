package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import scala.collection.immutable
import scala.collection.mutable
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_util.FileBasedObjectCache
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.Sorting

object CompactUndirectedGraph {
  def apply(rc: RuntimeContext,
            edges: EdgeBundleData,
            needsBothDirections: Boolean = true): CompactUndirectedGraph = {
    assert(edges.edgeBundle.isLocal, "Cannot create CUG from cross-graph edges.")
    val path = rc.broadcastDirectory / scala.util.Random.alphanumeric.take(10).mkString.toLowerCase
    val edgesRDD = edges.rdd
    val outEdges = edgesRDD.map {
      case (id, edge) => (edge.src, edge.dst)
    }.groupBySortedKey(edgesRDD.partitioner.get)
    val inEdges = edgesRDD.map {
      case (id, edge) => (edge.dst, edge.src)
    }.groupBySortedKey(edgesRDD.partitioner.get)
    val adjList = outEdges.fullOuterJoin(inEdges)
      .mapValuesWithKeys {
        case (v, (outs, ins)) => {
          val outSet = outs.getOrElse(Seq()).toSet
          val inSet = ins.getOrElse(Seq()).toSet
          val combined = if (needsBothDirections) (outSet & inSet) else (outSet | inSet)
          (combined - v).toArray.sorted
        }
      }
    log.info("CUG Writing...")
    adjList.context.runJob(adjList, (task, it: Iterator[(ID, Array[ID])]) =>
      {
        val neighbors = mutable.ArrayBuffer[ID]()
        val starts = mutable.ArrayBuffer[(ID, Int)]()
        var index = 0
        for ((v, ns) <- it) {
          neighbors ++= ns
          starts += v -> index
          index += ns.size
        }
        starts += 0L -> index // Sentinel.
        val dir = path / task.partitionId.toString
        (dir / "neighbors").createFromObjectKryo(neighbors.toArray)
        // "starts" is sorted because adjList is a SortedRDD.
        (dir / "starts").createFromObjectKryo(starts.toArray)
      })
    log.info("CUG Partitions written.")
    return CompactUndirectedGraph(path, adjList.partitioner.get)
  }
}

case class CompactUndirectedGraph(path: Filename, partitioner: spark.Partitioner) {
  @transient lazy val cache = Array.ofDim[CompactUndirectedGraphPartition](partitioner.numPartitions)

  def getNeighbors(vid: ID): Seq[ID] = {
    getPartition(vid).getNeighbors(vid)
  }

  def getPartition(vid: ID): CompactUndirectedGraphPartition = {
    val pid = partitioner.getPartition(vid)
    if (cache(pid) == null) {
      val dir = path / pid.toString
      val neighbors = FileBasedObjectCache.get[Array[ID]](dir / "neighbors")
      val starts = FileBasedObjectCache.get[Array[(ID, Int)]](dir / "starts")
      cache(pid) = new CompactUndirectedGraphPartition(neighbors, starts)
    }
    cache(pid)
  }
}

class CompactUndirectedGraphPartition(
    neighbors: Array[ID],
    starts: Array[(ID, Int)]) {

  def findId(id: ID): Int = {
    var lb = 0
    var ub = starts.size - 1 // The last element is the sentinel.

    while (lb < ub) {
      val mid = (lb + ub) / 2
      val (currId, currStart) = starts(mid)
      if (currId == id) {
        return mid
      } else if (currId > id) {
        ub = mid
      } else {
        lb = mid + 1
      }
    }
    return -1
  }

  def getNeighbors(vid: ID): Seq[ID] = {
    val idx = findId(vid)
    if (idx == -1) {
      Seq()
    } else {
      neighbors.view.slice(starts(idx)._2, starts(idx + 1)._2)
    }
  }
}
