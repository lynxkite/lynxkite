package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.{ Partition, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.ClassTag

object SortedRDD {
  // Creates a SortedRDD from an unsorted RDD.
  def fromUnsorted[K: Ordering, V](rdd: RDD[(K, V)]): SortedRDD[K, V] =
    new SortedRDD(
      rdd.mapPartitions(_.toIndexedSeq.sortBy(_._1).iterator, preservesPartitioning = true))

  // Wraps an already sorted RDD.
  def fromSorted[K: Ordering, V](rdd: RDD[(K, V)]): SortedRDD[K, V] =
    new SortedRDD(rdd)
}

// An RDD with each partition sorted by the key. "self" must already be sorted.
class SortedRDD[K: Ordering, V] private[spark_util] (self: RDD[(K, V)]) extends RDD[(K, V)](self) {
  override def getPartitions: Array[Partition] = self.partitions
  override val partitioner = self.partitioner
  override def compute(split: Partition, context: TaskContext) = self.compute(split, context)

  private def merge[K, V1, V2](
    bi1: collection.BufferedIterator[(K, V1)],
    bi2: collection.BufferedIterator[(K, V2)])(implicit ord: Ordering[K]): Stream[(K, (V1, V2))] = {
    if (bi1.hasNext && bi2.hasNext) {
      val (k1, v1) = bi1.head
      val (k2, v2) = bi2.head
      if (ord.equiv(k1, k2)) {
        bi1.next; bi2.next
        (k1, (v1, v2)) #:: merge(bi1, bi2)
      } else if (ord.lt(k1, k2)) {
        bi1.next
        merge(bi1, bi2)
      } else {
        bi2.next
        merge(bi1, bi2)
      }
    } else {
      Stream()
    }
  }

  def sortedJoin[V2](other: SortedRDD[K, V2]): SortedRDD[K, (V, V2)] = {
    assert(self.partitions.size == other.partitions.size, s"Size mismatch between $self and $other")
    assert(self.partitioner == other.partitioner, s"Partitioner mismatch between $self and $other")
    val zipped = this.zipPartitions(other, true) { (it1, it2) => merge(it1.buffered, it2.buffered).iterator }
    return new SortedRDD(zipped)
  }

  override def distinct: SortedRDD[K, V] = {
    val distinct = this.mapPartitions({ it =>
      val bi = it.buffered
      new Iterator[(K, V)] {
        def hasNext = bi.hasNext
        def next() = {
          val n = bi.next
          while (bi.hasNext && bi.head == n) bi.next
          n
        }
      }
    }, preservesPartitioning = true)
    return new SortedRDD(distinct)
  }

  def collectFirstNValues(n: Int)(implicit ct: ClassTag[V]): Seq[V] = {
    val numPartitions = partitions.size
    val div = n / numPartitions
    val mod = n % numPartitions
    this
      .mapPartitionsWithIndex((pid, it) => {
        val elementsFromThisPartition = if (pid < mod) (div + 1) else div
        it.take(elementsFromThisPartition)
      })
      .map { case (k, v) => v }
      .collect
  }
}
