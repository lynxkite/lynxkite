package com.lynxanalytics.biggraph.spark_util

import org.apache.spark
import org.apache.spark.rdd.RDD
import scala.reflect._

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._

object HybridRDD {
  private val hybridLookupThreshold =
    LoggedEnvironment.envOrElse("KITE_HYBRID_LOOKUP_THRESHOLD", "100000").toInt
  private val hybridLookupMaxLarge =
    LoggedEnvironment.envOrElse("KITE_HYBRID_LOOKUP_MAX_LARGE", "100").toInt
}

case class HybridRDD[K: Ordering: ClassTag, T: ClassTag](
    sourceRDD: RDD[(K, T)],
    maxValuesPerKey: Int = HybridRDD.hybridLookupThreshold) {

  val tops = {
    val ordering = new CountOrdering[K]
    sourceRDD
      .mapValues(x => 1L)
      .reduceByKey(_ + _)
      .top(HybridRDD.hybridLookupMaxLarge)(ordering)
      .sorted(ordering)
  }
  val isEmpty = tops.isEmpty
  val isSkewed = !tops.isEmpty && tops.last._2 > maxValuesPerKey

  val (largeKeysSet, largeKeysCoverage) = if (isEmpty || !isSkewed) {
    (Set.empty[K], 0L)
  } else {
    (tops.map(_._1).toSet, tops.map(_._2).reduce(_ + _))
  }
  val smallKeysTable = if (isSkewed) {
    sourceRDD.filter { case (key, _) => !largeKeysSet.contains(key) }
  } else {
    sourceRDD.context.emptyRDD[(K, T)]
  }

  def persist(storageLevel: spark.storage.StorageLevel): Unit = {
    if (!isEmpty) {
      if (isSkewed) {
        smallKeysTable.persist(storageLevel)
      }
      sourceRDD.persist(storageLevel)
    }
  }

  // A lookup method based on joining the source RDD with the lookup table. Assumes
  // that each key has only so many instances that we can handle all of them in a single partition.
  def joinLookup[K: Ordering: ClassTag, T: ClassTag, S](
    sourceRDD: RDD[(K, T)], lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    import Implicits._
    sourceRDD.sort(lookupTable.partitioner.get).sortedJoin(lookupTable)
  }

  // A lookup method based on sending the lookup table to all tasks. The lookup table should be
  // reasonably small.
  def smallTableLookup[K, T, S](
    sourceRDD: RDD[(K, T)], lookupTable: Map[K, S]): RDD[(K, (T, S))] = {

    sourceRDD
      .flatMap { case (key, tValue) => lookupTable.get(key).map(sValue => key -> (tValue, sValue)) }
  }

  // Same as hybridLookup but repartitions the result after a hybrid lookup.
  def hybridLookupAndRepartition[S](
    lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = hybridLookup(lookupTable)
    if (isSkewed) {
      result.repartition(sourceRDD.partitions.size)
    } else {
      result
    }
  }

  // Same as hybridLookup but coalesces the result after a hybrid lookup.
  def hybridLookupAndCoalesce[S](
    lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = hybridLookup(lookupTable)
    if (isSkewed) {
      result.coalesce(sourceRDD.partitions.size)
    } else {
      result
    }
  }

  // A lookup method that does smallTableLookup for a few keys that have too many instances to
  // be handled by joinLookup and does joinLookup for the rest.
  def hybridLookup[S](
    lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {

    if (isEmpty) sourceRDD.context.emptyRDD
    else {
      if (isSkewed) {
        val largeKeysMap = lookupTable.restrictToIdSet(tops.map(_._1).toIndexedSeq.sorted).collect.toMap
        log.info(s"Hybrid lookup found ${largeKeysSet.size} large keys covering "
          + "${largeKeysCoverage} source records.")
        val larges = smallTableLookup(sourceRDD, largeKeysMap)
        val smalls = joinLookup(smallKeysTable, lookupTable)
        smalls ++ larges
      } else {
        joinLookup(sourceRDD, lookupTable)
      }
    }
  }
}
