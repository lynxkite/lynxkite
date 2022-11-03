// Hybrid RDDs that combine different methods when working with skewed datasets.
//
// # The problem
//
// Take PageRank for example. In each iteration we have the current PageRank and we want to multiply
// it by the edge weights and add it up for each receiving vertex.
//
//   val edges = ... // src -> (dst, weight) RDD partitioned by src.
//   // In each iteration:
//   val incomingRank = edges.join(pageRank)
//     .map { case (src, ((dst, weight), pr)) => dst -> pr * weight }
//     .reduceByKey(_ + _)
//
// (Based on PageRank.scala.)
//
// The vertex and edge data (current PageRank and weight) have to meet so they can be multiplied.
// There are two ways to do this:
//
//  1. Partition the edges by the source vertex IDs as in the code snippet. Then each current PageRank
//     value needs only to be sent to one partition.
//
//  2. Partition the edges by the edge ID. Then the current PageRank values for all vertices have to be
//     sent to every partition. (Every partition may contain edges for any vertex.)
//
// Option 1 suffers when high-degree vertices create an uneven partitioning. Partitions that include
// edges starting from high-degree vertices will become hotspots.
//
// Option 2 is not generally feasible because the current PageRank values for all vertices may not
// fit on one machine. (Also this would just move an unreasonable amount of data around.)
//
// # The solution
//
// We use option 1 for the edges of the many low-degree vertices and option 2 for the edges of the
// few high-degree vertices.
//
// We draw the line by default at 20% of the partition size, but this is configurable. With our
// default partition size this means a degree of 40,000 puts you among the high-degree vertices.
//
// To find the high-degree vertices, we may compute the actual degrees. But often we know that the
// original partitioning (the edge partitioning) is even and unrelated to the vertex IDs. In that
// case it is enough to look at a sample of the partitions, making this relatively cheap.
//
// The number of high-degree vertices is small (with the above settings, at most five times the
// number of edge partitions). We can broadcast the list to create two RDDs:
//
//  1. One that contains the edges of low-degree vertices. We repartition this by source vertex ID.
//  2. One that contains the edges of high-degree vertices. We leave this partitioned by edge ID.
//
// We can then use the normal merge join (option 1 above) on the first RDD. It no longer has
// hotspots. We can use broadcast join (option 2 above) on the second RDD. It may include a lot of
// edge data, but only a small number of vertices. Then we just concatenate the resulting RDDs.
//
// Once built, the hybrid RDD can be reused in one operation (e.g. multiple iterations of PageRank)
// or even across multiple operations. (See HybridBundle.scala.)
//
// The next step in PageRank where we add up the PageRank values for each vertex
// (“reduceByKey(_ + _)”) is well handled by Spark out of the box. Even for high-degree vertices one
// number per partition will at most be transferred due to map-side aggregation.
//
package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext

import scala.reflect._

import com.lynxanalytics.biggraph.{logger => log}
import com.lynxanalytics.biggraph.graph_api.io.EntityIO

object HybridRDD {
  // If the threshold is not defined use a number sufficiently smaller than vertices per partition.
  // If we pick vertices per partition the partition load is going to be uneven for the sortedJoin
  // in joinLookup. If we pick a too small threshold the largeKeysSet is going to be too large.
  // In the worst case largeKeysSet can have sourceRDD size / threshold elements.
  private val hybridLookupThreshold = util.Properties.envOrElse(
    "KITE_HYBRID_LOOKUP_THRESHOLD",
    s"${EntityIO.verticesPerPartition / 5}").toInt

  // A lookup method based on joining the source RDD with the lookup table. Assumes
  // that each key has only so many instances that we can handle all of them in a single partition.
  private def joinLookup[K: Ordering: ClassTag, T: ClassTag, S](
      leftRDD: SortedRDD[K, T],
      lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    assert(
      leftRDD.partitioner.get eq lookupRDD.partitioner.get,
      "LeftRDD and lookupRDD must have the same partitioner.")
    leftRDD.sortedJoin(lookupRDD)
  }

  // A lookup method based on sending the lookup table to all tasks. The lookup table should be
  // reasonably small.
  private def smallTableLookup[K: Ordering: ClassTag, T: ClassTag, S](
      leftRDD: RDD[(K, T)],
      lookupTable: Map[K, S]): RDD[(K, (T, S))] = {
    leftRDD
      .flatMap { case (key, tValue) => lookupTable.get(key).map(sValue => key -> (tValue, sValue)) }
  }

  def of[K: Ordering: ClassTag, T: ClassTag](
      // The large potentially skewed RDD to do joins on.
      sourceRDD: RDD[(K, T)],
      // A partitioner good enough for the sourceRDD. All RDDs used in the lookup methods
      // must have the same partitioner.
      partitioner: spark.Partitioner,
      // The RDD is distributed evenly, both in terms of the sizes of the partitions and the
      // distribution of the keys per partition.
      even: Boolean,
      // The threshold to decide whether this HybridRDD is skewed.
      threshold: Int = HybridRDD.hybridLookupThreshold)(
      implicit rc: RuntimeContext): HybridRDD[K, T] = {

    val larges = {
      val numPartitions = sourceRDD.partitions.size
      val (rdd, sampleRatio) =
        if (even && numPartitions > 0) {
          // Assumes that the keys are distributed evenly among the partitions.
          val numSamplePartitions = rc.numSamplePartitions min numPartitions
          (new PartialRDD(sourceRDD, numSamplePartitions), numPartitions.toDouble / numSamplePartitions)
        } else {
          (sourceRDD, 1.0)
        }
      rdd
        .mapValues(_ => 1L)
        .reduceByKey(_ + _)
        .mapValues(x => (x * sampleRatio).toLong)
        .filter(_._2 > threshold)
        .collect
    }

    // True iff this HybridRDD has keys with large cardinalities.
    val isSkewed = !larges.isEmpty
    val largeKeysSet = larges.map(_._1).toSet

    // The RDD containing only keys that are safe to use in sorted join.
    import Implicits._
    val smallKeysRDD: SortedRDD[K, T] = {
      if (isSkewed) {
        sourceRDD.filter { case (key, _) => !largeKeysSet.contains(key) }
      } else {
        sourceRDD
      }
    }.sort(partitioner)
    // The RDD to use with map lookup. It may contain keys with large cardinalities.
    val largeKeysRDD: Option[RDD[(K, T)]] =
      if (isSkewed) {
        // We need to guarantee that largeKeysRDD has the correct amount of partitions.
        val largeKeysRDD =
          if (sourceRDD.partitions.size == partitioner.numPartitions) {
            // Let's not perform expensive repartitioning if avoidable.
            sourceRDD
          } else {
            sourceRDD.repartition(partitioner.numPartitions)
          }.filter { case (key, _) => largeKeysSet.contains(key) }
        Some(largeKeysRDD)
      } else {
        None
      }

    HybridRDD(largeKeysRDD, smallKeysRDD, larges)
  }
}

// A wrapping class for potentially skewed RDDs. Skewed means the cardinality of keys
// is extremely unevenly distributed.
case class HybridRDD[K: Ordering: ClassTag, T: ClassTag](
    // The large potentially skewed RDD to do joins on.
    largeKeysRDD: Option[RDD[(K, T)]],
    smallKeysRDD: SortedRDD[K, T],
    larges: Seq[(K, Long)])
    extends RDD[(K, T)](
      smallKeysRDD.sparkContext,
      Seq(new spark.OneToOneDependency(smallKeysRDD)) ++ largeKeysRDD.map(new spark.OneToOneDependency(_))) {

  // True iff this HybridRDD has keys with large cardinalities.
  val isSkewed = !largeKeysRDD.isEmpty
  val resultPartitioner = smallKeysRDD.partitioner.get
  override val partitioner = if (isSkewed) None else Some(resultPartitioner)

  if (isSkewed) {
    assert(largeKeysRDD.get.partitions.size == resultPartitioner.numPartitions)
  }

  override def getPartitions: Array[Partition] = smallKeysRDD.partitions
  override def compute(split: Partition, context: TaskContext) =
    smallKeysRDD.iterator(split, context) ++
      largeKeysRDD.map(_.iterator(split, context)).getOrElse(Iterator())

  val (largeKeysSet, largeKeysCoverage) =
    if (!isSkewed) {
      (Set.empty[K], 0L)
    } else {
      (larges.map(_._1).toSet, larges.map(_._2).reduce(_ + _))
    }

  // Caches the smallKeysRDD and the largeKeysRDD for skewed HybridRDDs.
  override def persist(storageLevel: spark.storage.StorageLevel): HybridRDD.this.type = {
    if (isSkewed) {
      largeKeysRDD.get.persist(storageLevel)
    }
    smallKeysRDD.persist(storageLevel)
    this
  }

  // Same as lookup but repartitions the result after a hybrid lookup. The elements of the
  // result RDD are evenly distributed among its partitions.
  def lookupAndRepartition[S](
      lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = lookup(lookupRDD)
    if (isSkewed) {
      // "ord = null" is a workaround for a Scala 2.11 compiler crash bug.
      // TODO: Remove when upgrading to 2.12.
      result.repartition(resultPartitioner.numPartitions)(ord = null)
    } else {
      result
    }
  }

  // A lookup method that does smallTableLookup for a few keys that have too many instances to
  // be handled by joinLookup and does joinLookup for the rest. There are no guarantees about the
  // partitions of the result RDD.
  def lookup[S](
      lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {

    val smalls = HybridRDD.joinLookup(smallKeysRDD, lookupRDD)
    if (isSkewed) {
      val largeKeysMap = lookupRDD.filter(largeKeysSet contains _._1).collect.toMap
      log.info(s"Hybrid lookup found ${largeKeysSet.size} large keys covering "
        + s"${largeKeysCoverage} source records.")
      val larges = HybridRDD.smallTableLookup(largeKeysRDD.get, largeKeysMap)
      smalls ++ larges
    } else {
      smalls // For non-skewed RDDs every row is in smallKeysRDD.
    }
  }
}
