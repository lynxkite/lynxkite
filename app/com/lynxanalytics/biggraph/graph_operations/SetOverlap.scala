package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._

// Generates edges between vertices by the amount of overlap in an attribute.
object SetOverlap {
  // Maximum number of sets to be O(n^2) compared.
  val SetListBruteForceLimit = 70
}
case class SetOverlap(minOverlap: Int) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vs)
    .inputVertexSet('sets)
    .inputEdgeBundle('links, 'vs -> 'sets)
    .outputEdgeBundle('overlaps, 'sets -> 'sets)
    // The generated edges have a single attribute, the overlap size.
    .outputEdgeAttribute[Int]('overlap_size, 'overlaps)

  // Set-valued attributes are represented as sorted Array[ID].
  type Set = Array[ID]
  // When dealing with multiple sets, they are identified by their VertexIds.
  type Sets = Iterable[(ID, Array[ID])]

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val partitioner = rc.defaultPartitioner

    val sets = inputs.edgeBundles('links).rdd.values
      .map { case Edge(vId, setId) => setId -> vId }
      .groupByKey(partitioner)

    type SetsByPrefix = RDD[(Seq[ID], Sets)]
    // Start with prefixes of length 1.
    var short: SetsByPrefix = rc.sparkContext.emptyRDD[(Seq[ID], Sets)]
    var long: SetsByPrefix = sets.flatMap {
      case (setId, set) => set.map(i => (Seq(i), (setId, set.toSeq.sorted.toArray)))
    }.groupByKey(partitioner)
    // Increase prefix length until all set lists are short.
    // We cannot use a prefix longer than minOverlap.
    for (iteration <- (2 to minOverlap)) {
      // Move over short lists.
      short ++= long.filter(_._2.size <= SetOverlap.SetListBruteForceLimit)
      long = long.filter(_._2.size > SetOverlap.SetListBruteForceLimit)
      // Increase prefix length.
      long = long.flatMap {
        case (prefix, sets) => sets.flatMap {
          case (setId, set) => {
            set
              .filter(node => node > prefix.last)
              .map(next => (prefix :+ next, (setId, set)))
          }
        }
      }.groupByKey(partitioner)
    }
    // Accept the remaining large set lists. We cannot split them further.
    short ++= long

    val edgesWithOverlaps: RDD[(Edge, Int)] = short.flatMap {
      case (prefix, sets) => edgesFor(prefix, sets)
    }

    val numberedEdgesWithOverlaps = RDDUtils.fastNumbered(edgesWithOverlaps).partitionBy(rc.defaultPartitioner)

    outputs.putEdgeBundle(
      'overlaps, numberedEdgesWithOverlaps.map { case (eId, (edge, overlap)) => eId -> edge })
    outputs.putEdgeAttribute(
      'overlap_size, numberedEdgesWithOverlaps.map { case (eId, (edge, overlap)) => eId -> overlap })
  }

  // Generates the edges for a set of sets. This is O(n^2), but the set should
  // be small.
  protected def edgesFor(prefix: Seq[ID], sets: Sets): Seq[(Edge, Int)] = {
    val setSeq = sets.toSeq
    for {
      (vid1, set1) <- setSeq
      (vid2, set2) <- setSeq
      overlap = SortedIntersectionSize(set1, set2, prefix)
      if vid1 != vid2 && overlap >= minOverlap
    } yield (Edge(vid1, vid2), overlap)
  }

  // Intersection size calculator. The same a-b pair will show up under multiple
  // prefixes if they have more nodes in common than the prefix length. To avoid
  // reporting them multiple times, SortedIntersectionSize() returns 0 unless
  // `pref` is a prefix of the overlap.
  protected def SortedIntersectionSize(
    a: Array[ID], b: Array[ID], pref: Seq[ID]): Int = {
    var ai = 0
    var bi = 0
    val prefi = pref.iterator
    var res = 0
    while (ai < a.length && bi < b.length) {
      if (a(ai) == b(bi)) {
        if (prefi.hasNext && a(ai) != prefi.next) {
          return 0 // `pref` is not a prefix of the overlap.
        }
        res += 1
        ai += 1
        bi += 1
      } else if (a(ai) < b(bi)) {
        ai += 1
      } else {
        bi += 1
      }
    }
    return res
  }
}
