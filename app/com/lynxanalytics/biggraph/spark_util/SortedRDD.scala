package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.{ Partition, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object SortedRDD {
  // Creates a SortedRDD from an unsorted RDD.
  def apply[K: Ordering, V](rdd: RDD[(K, V)]): SortedRDD[K, V] =
    new SortedRDD(rdd.mapPartitions(_.toSeq.sortBy(_._1).iterator, preservesPartitioning = true))
}

// An RDD with each partition sorted by the key. "self" must already be sorted.
class SortedRDD[K: Ordering, V] (self: RDD[(K, V)]) extends RDD[(K, V)](self) {
  override def getPartitions: Array[Partition] = self.partitions
  override val partitioner = self.partitioner
  override def compute(split: Partition, context: TaskContext) = self.compute(split, context)

  private def merge[K, V](
    bi1: collection.BufferedIterator[(K, V)],
    bi2: collection.BufferedIterator[(K, V)])(implicit ord: Ordering[K]): Stream[(K, (V, V))] = {
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

  def join(other: SortedRDD[K, V]): SortedRDD[K, (V, V)] = {
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
}
