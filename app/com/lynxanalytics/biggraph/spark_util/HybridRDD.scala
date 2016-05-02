// Assorted utilities for working with RDDs.
package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import org.apache.spark.rdd.RDD
import scala.reflect._

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object HybridRDD {
  private val hybridLookupThreshold =
    LoggedEnvironment.envOrElse("KITE_HYBRID_LOOKUP_THRESHOLD", "100000").toInt
  private val hybridLookupMaxLarge =
    LoggedEnvironment.envOrElse("KITE_HYBRID_LOOKUP_MAX_LARGE", "100").toInt
}

// A wrapping class for potentially skewed RDDs. Skewed means the cardinality of keys
// is extremely unevenly distributed.
case class HybridRDD[K: Ordering: ClassTag, T: ClassTag](
    // The large potentially skewed RDD to do joins on.
    sourceRDD: RDD[(K, T)],
    // A optional partitioner good enough for both RDDs. Only specify it if the subsequent
    // lookupRDDs are partitioned with the same partitioner.
    partitioner: Option[spark.Partitioner] = None,
    maxValuesPerKey: Int = HybridRDD.hybridLookupThreshold) {

  val tops = {
    val ordering = new CountOrdering[K]
    sourceRDD
      .mapValues(x => 1L)
      .reduceByKey(_ + _)
      .top(HybridRDD.hybridLookupMaxLarge)(ordering)
      .sorted(ordering)
  }
  // True iff this HybridRDD has no elements.
  val isEmpty = tops.isEmpty
  // True iff this HybridRDD has keys with large cardinalities.
  val isSkewed = !tops.isEmpty && tops.last._2 > maxValuesPerKey
  // True iff a partitioner is specified.
  val hasPartitioner = !partitioner.isEmpty

  val (largeKeysSet, largeKeysCoverage) = if (isEmpty || !isSkewed) {
    (Set.empty[K], 0L)
  } else {
    (tops.map(_._1).toSet, tops.map(_._2).reduce(_ + _))
  }
  val smallKeysRDD = if (isSkewed) {
    val rdd = sourceRDD.filter { case (key, _) => !largeKeysSet.contains(key) }
    if (hasPartitioner) {
      // Sort the smallKeysRDD once instead of every time there is a lookup.
      import Implicits._
      rdd.sort(partitioner.get)
    } else {
      rdd
    }
  } else {
    sourceRDD.context.emptyRDD[(K, T)]
  }
  val optimizedSourceRDD = if (!isSkewed && hasPartitioner) {
    // If this HybridRDD is not skewed joinLookups will happen on the sourceRDD, so better sort it once.
    import Implicits._
    sourceRDD.sort(partitioner.get)
  } else {
    sourceRDD
  }

  // Caches the optimizedSourceRDD and the smallKeysRDD.
  def persist(storageLevel: spark.storage.StorageLevel): Unit = {
    if (!isEmpty) {
      if (isSkewed) {
        smallKeysRDD.persist(storageLevel)
      }
      optimizedSourceRDD.persist(storageLevel)
    }
  }

  // A lookup method based on joining the source RDD with the lookup table. Assumes
  // that each key has only so many instances that we can handle all of them in a single partition.
  private def joinLookup[S](
    sourceRDD: RDD[(K, T)], lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    import Implicits._
    // Sort will have no effect if the sourceRDD is already sorted with the same partitioner.
    sourceRDD.sort(lookupRDD.partitioner.get).sortedJoin(lookupRDD)
  }

  // A lookup method based on sending the lookup table to all tasks. The lookup table should be
  // reasonably small.
  private def smallTableLookup[S](
    sourceRDD: RDD[(K, T)], lookupTable: Map[K, S]): RDD[(K, (T, S))] = {
    sourceRDD
      .flatMap { case (key, tValue) => lookupTable.get(key).map(sValue => key -> (tValue, sValue)) }
  }

  // Same as lookup but repartitions the result after a hybrid lookup. The elements of the
  // result RDD are evenly distributed among its partitions.
  def lookupAndRepartition[S](
    lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = lookup(lookupRDD)
    if (isSkewed) {
      // "ord = null" is a workaround for a Scala 2.10 compiler bug.
      // TODO: Remove when upgrading to 2.11.
      result.repartition(sourceRDD.partitions.size)(ord = null)
    } else {
      result
    }
  }

  // Same as lookup but coalesces the result after a hybrid lookup. The result RDD has
  // as many partitions as the original one.
  def lookupAndCoalesce[S](
    lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = lookup(lookupRDD)
    if (isSkewed) {
      // "ord = null" is a workaround for a Scala 2.10 compiler bug.
      // TODO: Remove when upgrading to 2.11.
      result.coalesce(sourceRDD.partitions.size)(ord = null)
    } else {
      result
    }
  }

  // A lookup method that does smallTableLookup for a few keys that have too many instances to
  // be handled by joinLookup and does joinLookup for the rest. There are no guarantees about the
  // partitions of the result RDD.
  def lookup[S](
    lookupRDD: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {

    if (isEmpty) sourceRDD.context.emptyRDD
    else {
      if (isSkewed) {
        val largeKeysMap = lookupRDD.restrictToIdSet(tops.map(_._1).toIndexedSeq.sorted).collect.toMap
        log.info(s"Hybrid lookup found ${largeKeysSet.size} large keys covering "
          + "${largeKeysCoverage} source records.")
        val larges = smallTableLookup(optimizedSourceRDD, largeKeysMap)
        val smalls = joinLookup(smallKeysRDD, lookupRDD)
        smalls ++ larges
      } else {
        joinLookup(optimizedSourceRDD, lookupRDD)
      }
    }
  }
}
