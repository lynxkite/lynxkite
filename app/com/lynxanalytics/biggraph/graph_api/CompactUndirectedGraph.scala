package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import scala.collection.immutable
import scala.collection.mutable
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_util.Filename
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
        val indices = mutable.ArrayBuffer[(ID, Int)]()
        val starts = mutable.ArrayBuffer[Int]()
        var index = 0
        for ((v, ns) <- it) {
          neighbors ++= ns
          indices += v -> indices.size
          starts += index
          index += ns.size
        }
        starts += index // Sentinel.
        val indicesArray = indices.toArray
        Sorting.quickSort(indicesArray)
        val dir = path / task.partitionId.toString
        (dir / "neighbors").createFromObjectKryo(neighbors.toArray)
        (dir / "indices").createFromObjectKryo(indicesArray)
        (dir / "starts").createFromObjectKryo(starts.toArray)
      })
    log.info("CUG Partitions written.")
    return new CompactUndirectedGraph(path, adjList.partitioner.get)
  }
}

class CompactUndirectedGraph(path: Filename, partitioner: spark.Partitioner) extends Serializable {
  @transient lazy val cache = Array.ofDim[CompactUndirectedGraphPartition](partitioner.numPartitions)

  def getNeighbors(vid: ID): Seq[ID] = {
    getPartition(vid).getNeighbors(vid)
  }

  def getPartition(vid: ID): CompactUndirectedGraphPartition = {
    val pid = partitioner.getPartition(vid)
    if (cache(pid) == null) {
      val dir = path / pid.toString
      val neighbors = (dir / "neighbors").loadObjectKryo.asInstanceOf[Array[ID]]
      val indices = (dir / "indices").loadObjectKryo.asInstanceOf[Array[(ID, Int)]]
      val starts = (dir / "starts").loadObjectKryo.asInstanceOf[Array[Int]]
      cache(pid) = new CompactUndirectedGraphPartition(neighbors, indices, starts)
    }
    cache(pid)
  }
}

class CompactUndirectedGraphPartition(
    neighbors: Array[ID],
    indices: Array[(ID, Int)],
    starts: Array[Int]) {

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

  def getNeighbors(vid: ID): Seq[ID] = {
    val idx = findId(vid)
    if (idx == -1) {
      Seq()
    } else {
      neighbors.view.slice(starts(idx), starts(idx + 1))
    }
  }
}
