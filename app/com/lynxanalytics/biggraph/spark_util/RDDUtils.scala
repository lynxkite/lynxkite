// Assorted utilities for working with RDDs.
package com.lynxanalytics.biggraph.spark_util

import com.esotericsoftware.kryo
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import scala.collection.mutable
import scala.reflect._

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._

// A container for storing ID counts per bucket and a sample.
class IDBuckets[T](
  val counts: mutable.Map[T, Long] = mutable.Map[T, Long]().withDefaultValue(0))
    extends Serializable with Equals {
  var sample = mutable.Map[ID, T]() // May be null!
  def add(id: ID, t: T): Unit = {
    counts(t) += 1
    addSample(id, t)
  }
  def absorb(b: IDBuckets[T]): Unit = {
    for ((k, v) <- b.counts) {
      counts(k) += v
    }
    if (b.sample == null) {
      sample = null
    } else for ((id, t) <- b.sample) {
      addSample(id, t)
    }
  }

  override def equals(that: Any): Boolean = {
    if (canEqual(that)) {
      val other = that.asInstanceOf[IDBuckets[T]]
      counts == other.counts && sample == other.sample
    } else
      false
  }
  override def canEqual(that: Any): Boolean = {
    that.isInstanceOf[IDBuckets[T]]
  }

  private def addSample(id: ID, t: T) = {
    if (sample != null) {
      sample(id) = t
      if (sample.size > IDBuckets.MaxSampleSize) {
        sample = null
      }
    }
  }
}
object IDBuckets {
  val MaxSampleSize = 50
}

// An Ordering defined on pairs of type (T, Long) which we consider as
// (entity, count of that entity) pairs. According to this ordering the pair with higher count is
// considered greater. This is useful for selecting the highest count objects from some collection.
class CountOrdering[T] extends Ordering[(T, Long)] with Serializable {
  def compare(x: (T, Long), y: (T, Long)): Int = {
    val (xk, xc) = x
    val (yk, yc) = y
    val xh = xk.hashCode
    val yh = yk.hashCode

    if (xc < yc) -1
    else if (xc > yc) 1
    else if (xh < yh) -1
    else if (xh > yh) 1
    else 0
  }
}

object RDDUtils {
  val threadLocalKryo = new ThreadLocal[kryo.Kryo] {
    override def initialValue() = BigGraphSparkContext.createKryoWithForcedRegistration()
  }

  def serialize[T](obj: Any): Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close
    bos.toByteArray
  }

  def kryoDeserialize[T](bytes: Array[Byte]): T = {
    val ois = new kryo.io.Input(bytes)
    threadLocalKryo.get.readClassAndObject(ois).asInstanceOf[T]
  }

  def kryoSerialize[T](obj: Any): Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream
    val oos = new kryo.io.Output(bos)
    try {
      threadLocalKryo.get.writeClassAndObject(oos, obj)
    } catch {
      case e: Exception =>
        log.error(
          s"Serialization error. We were trying to serialize: $obj of class ${obj.getClass}")
        throw e
    }
    oos.close
    bos.toByteArray
  }

  /*
   * For a filtered RDD computes how many elements were "skipped" by the filter.
   *
   * The input is two sorted RDDs, restricted and full, where the keys in restricted form a subset
   * of the keys in full. The function will return the data in restricted complemented by an extra
   * integer for each key. This integer is the number of keys found in fullRDD between
   * the actual key (inclusive) and the previous key in restricted (exclusive). Summing up that
   * integer on a per-partition prefix gives an estimate of how much of the original, full RDD
   * we had to process to get the filtered sample we needed. This is necessary to be able to
   * estimate totals from the filtered numbers.
   */
  private def unfilteredCounts[T](
    full: SortedRDD[ID, _], restricted: SortedRDD[ID, T]): RDD[(ID, (T, Int))] = {
    full.zipPartitions(restricted, true) { (fit, rit) =>
      new Iterator[(ID, (T, Int))] {
        def hasNext = rit.hasNext
        def next() = {
          val nxt = rit.next
          var c = 1
          while (fit.next._1 < nxt._1) c += 1
          (nxt._1, (nxt._2, c))
        }
      }
    }
  }

  private def estimateValueCounts[T](
    fullRDD: SortedRDD[ID, _],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int,
    rc: RuntimeContext): IDBuckets[T] = {

    import Implicits._
    val withCounts = unfilteredCounts(fullRDD, data)
    val sampleWithCounts = withCounts.coalesce(rc).takeFirstNValuesOrSo(requiredPositiveSamples)
    val (valueBuckets, unfilteredCount, filteredCount) = sampleWithCounts
      .aggregate((
        new IDBuckets[T]() /* observed value counts */ ,
        0 /* estimated total count corresponding to the observed filtered sample */ ,
        0 /* observed filtered sample size */ ))(
        {
          case ((buckets, uct, fct), (id, (value, uc))) =>
            buckets.add(id, value)
            (buckets, uct + uc, fct + 1)
        },
        {
          case ((buckets1, uct1, fct1), (buckets2, uct2, fct2)) =>
            buckets1.absorb(buckets2)
            (buckets1, uct1 + uct2, fct1 + fct2)
        })
    // Extrapolate from sample.
    val multiplier = if (filteredCount < requiredPositiveSamples / 2) {
      // No sampling must have happened.
      1.0
    } else {
      totalVertexCount * 1.0 / unfilteredCount
    }
    valueBuckets.counts.transform { (value, count) => (multiplier * count).toInt }
    // Round to next power of 10.
    // TODO: Move this closer to the UI.
    val rounder = math.pow(10, math.ceil(math.log10(multiplier))).toInt
    valueBuckets.counts.transform {
      (value, count) => math.round(count / rounder).toInt * rounder
    }
    valueBuckets
  }

  private def preciseValueCounts[T](
    data: SortedRDD[ID, T]): IDBuckets[T] = {

    data.aggregate(new IDBuckets[T]())(
      {
        case (buckets, (id, value)) =>
          buckets.add(id, value)
          buckets
      },
      {
        case (buckets1, buckets2) =>
          buckets1.absorb(buckets2)
          buckets1
      })
  }

  def estimateOrPreciseValueCounts[T](
    fullRDD: SortedRDD[ID, _],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int,
    rc: RuntimeContext): IDBuckets[T] = {

    if (requiredPositiveSamples < 0) {
      preciseValueCounts(data)
    } else {
      estimateValueCounts(
        fullRDD,
        data,
        totalVertexCount,
        requiredPositiveSamples,
        rc)
    }
  }

  def estimateValueWeights[T](
    fullRDD: SortedRDD[ID, _],
    weightsRDD: UniqueSortedRDD[ID, Double],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int,
    rc: RuntimeContext): Map[T, Double] = {

    import Implicits._
    val withWeights = data.sortedJoin(weightsRDD)
    val withWeightsAndCounts = unfilteredCounts(fullRDD, withWeights)
    val sampleWithWeightsAndCounts =
      withWeightsAndCounts.coalesce(rc).takeFirstNValuesOrSo(requiredPositiveSamples)
    val (valueWeights, unfilteredCount, filteredCount) = withWeightsAndCounts
      .values
      .aggregate((
        mutable.Map[T, Double]() /* observed value weights */ ,
        0 /* estimated total count corresponding to the observed filtered sample */ ,
        0 /* observed filtered sample size */ ))(
        {
          case ((map, uct, fct), ((key, weight), uc)) =>
            incrementWeightMap(map, key, weight)
            (map, uct + uc, fct + 1)
        },
        {
          case ((map1, uct1, fct1), (map2, uct2, fct2)) =>
            for ((k, v) <- map2) {
              incrementWeightMap(map1, k, v)
            }
            (map1, uct1 + uct2, fct1 + fct2)
        })
    val multiplier = if (filteredCount < requiredPositiveSamples / 2) {
      // No sampling must have happened.
      1.0
    } else {
      totalVertexCount * 1.0 / unfilteredCount
    }
    valueWeights.toMap.map { case (k, c) => k -> multiplier * c }
  }

  def incrementMap[K](map: mutable.Map[K, Int], key: K, increment: Int = 1): Unit = {
    map(key) = if (map.contains(key)) (map(key) + increment) else increment
  }

  def incrementWeightMap[K](map: mutable.Map[K, Double], key: K, increment: Double): Unit = {
    map(key) = if (map.contains(key)) (map(key) + increment) else increment
  }
}

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
  private def joinLookup[S](
    sourceRDD: RDD[(K, T)], lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    import Implicits._
    sourceRDD.sort(lookupTable.partitioner.get).sortedJoin(lookupTable)
  }

  // A lookup method based on sending the lookup table to all tasks. The lookup table should be
  // reasonably small.
  private def smallTableLookup[S](
    sourceRDD: RDD[(K, T)], lookupTable: Map[K, S]): RDD[(K, (T, S))] = {

    sourceRDD
      .flatMap { case (key, tValue) => lookupTable.get(key).map(sValue => key -> (tValue, sValue)) }
  }

  // Same as hybridLookup but repartitions the result after a hybrid lookup.
  def lookupAndRepartition[S](
    lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = lookup(lookupTable)
    if (isSkewed) {
      result.repartition(sourceRDD.partitions.size)
    } else {
      result
    }
  }

  // Same as hybridLookup but coalesces the result after a hybrid lookup.
  def lookupAndCoalesce[S](
    lookupTable: UniqueSortedRDD[K, S]): RDD[(K, (T, S))] = {
    val result = lookup(lookupTable)
    if (isSkewed) {
      result.coalesce(sourceRDD.partitions.size)
    } else {
      result
    }
  }

  // A lookup method that does smallTableLookup for a few keys that have too many instances to
  // be handled by joinLookup and does joinLookup for the rest.
  def lookup[S](
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

object Implicits {
  // Used by RDDUtils.fastNumbered to generate IDs.
  // Args:
  //   parts: The number of partitions.
  //   part: Current partition index.
  //   row: Current row index.
  private[spark_util] def genID(parts: Int, part: Int, row: Int): Long = {
    // HashPartitioner will use nonNegativeMod(id.hashCode, parts) to pick the partition.
    // We generate the ID such that nonNegativeMod(id.hashCode, parts) == part.
    val longID = parts.toLong * row.toLong + part.toLong
    val low = Int.MaxValue % parts
    val high = Int.MinValue % parts
    // Correction for overflows. No correction needed for underflows.
    // low + 1 ≡ high + jump  (mod parts)
    val jump = (low + 1 - high + parts * 2) % parts
    val period = Int.MaxValue.toLong * 2L - jump // Distance between overflows.
    val offset = Int.MaxValue.toLong - jump // Zero is at this point in the period.
    val jumps = (longID + offset) / period
    val jumped = longID + jump * jumps
    jumped ^ (jumped >>> 32) // Cancel out the bit flips in Long.hashCode.
  }

  implicit class AnyRDDUtils[T: ClassTag](self: RDD[T]) {
    def numbered: RDD[(Long, T)] = {
      val localCounts = self.glom().map(_.size).collect().scan(0)(_ + _)
      val counts = self.sparkContext.broadcast(localCounts)
      self.mapPartitionsWithIndex((i, p) => {
        (counts.value(i) until counts.value(i + 1)).map(_.toLong).toIterator zip p
      })
    }

    // Adds unique ID numbers to rows of an RDD as a transformation.
    def randomNumbered(numPartitions: Int = self.partitions.size): UniqueSortedRDD[ID, T] = {
      val partitioner = new spark.HashPartitioner(numPartitions)
      randomNumbered(partitioner)
    }

    def randomNumbered(partitioner: spark.Partitioner): UniqueSortedRDD[ID, T] = {
      // generate a random id for the hash
      val numPartitions = self.partitions.size
      self.mapPartitionsWithIndex({
        case (pid, it) =>
          val rnd = new scala.util.Random(pid)
          var uniqueID = pid.toLong - numPartitions
          it.map { value =>
            val randomID = rnd.nextInt.toLong
            uniqueID += numPartitions
            // The ID computed here is guaranteed unique as long as uniqueID fits in one unsigned
            // int. Otherwise it's still unique with large probability.
            ((randomID << 32) ^ uniqueID) -> value
          }
      }).sortUnique(partitioner)
    }

    // Cheap method to force an RDD calculation
    def calculate() = self.foreach(_ => ())

    def printDetails(indent: Int = 0): Unit = {
      println(" " * indent + s"- $self (${self.partitions.size} partitions)")
      for (dep <- self.dependencies) {
        dep.rdd.printDetails(indent + 1)
      }
    }

    // Returns an RDD that only contains as many partitions as there are available cores.
    def coalesce(rc: RuntimeContext): RDD[T] =
      self.coalesce(rc.sparkContext.defaultParallelism)

    // Take a sample of approximately the given size.
    def takeFirstNValuesOrSo(n: Int): RDD[T] = {
      val numPartitions = self.partitions.size
      val div = n / numPartitions
      val mod = n % numPartitions
      self.mapPartitionsWithIndex(
        { (pid, it) =>
          val elementsFromThisPartition = if (pid < mod) (div + 1) else div
          it.take(elementsFromThisPartition)
        },
        preservesPartitioning = true)
    }
  }

  implicit class PairRDDUtils[K: Ordering, V](self: RDD[(K, V)]) extends Serializable {
    // Trust that this RDD is partitioned and sorted.
    def asSortedRDD(implicit ck: ClassTag[K], cv: ClassTag[V]): SortedRDD[K, V] = {
      assert(self.partitioner.isDefined,
        s"Cannot cast to unique RDD if there is no partitioner specified")
      new AlreadySortedRDD[K, V](self)
    }
    // Trust that this RDD is partitioned and sorted and has unique keys.
    // Make sure it uses the given partitioner.
    def asUniqueSortedRDD(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {
      assert(self.partitions.size == partitioner.numPartitions,
        s"Cannot apply partitioner of size ${partitioner.numPartitions}" +
          s" to RDD of size ${self.partitions.size}: $self")
      new AlreadyPartitionedRDD[(K, V)](self, partitioner).asUniqueSortedRDD
    }
    def asUniqueSortedRDD(implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {
      assert(self.partitioner.isDefined,
        s"Cannot cast to unique RDD if there is no partitioner specified")
      new AlreadySortedRDD[K, V](self) with UniqueSortedRDD[K, V]
    }
    // Sorts each partition of the RDD in isolation.
    def sort(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): SortedRDD[K, V] = {
      self match {
        case self: SortedRDD[K, V] if partitioner eq self.partitioner.get =>
          self
        case self =>
          // Use ShuffledRDD instead of partitionBy to avoid re-using an equal but non-identical
          // partitioner.
          val rawRDD = new spark.rdd.ShuffledRDD[K, V, V](self, partitioner)
          makeShuffledRDDSorted(rawRDD)
          rawRDD.asSortedRDD
      }
    }

    // Sorts each partition of the RDD in isolation and trusts that keys are unique.
    def sortUnique(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {
      self match {
        case self: UniqueSortedRDD[K, V] if partitioner eq self.partitioner.get =>
          self
        case self: SortedRDD[K, V] if partitioner eq self.partitioner.get =>
          self.asUniqueSortedRDD
        case self =>
          // Use ShuffledRDD instead of partitionBy to avoid re-using an equal but non-identical
          // partitioner.
          val rawRDD = new spark.rdd.ShuffledRDD[K, V, V](self, partitioner)
          makeShuffledRDDSorted(rawRDD)
          rawRDD.asUniqueSortedRDD
      }
    }

    def groupBySortedKey(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, Iterable[V]] = {

      if (self.isInstanceOf[SortedRDD[K, V]]) {
        self.asInstanceOf[SortedRDD[K, V]].groupByKey()
      } else {
        val rawRDD = self.groupByKey(partitioner)
        makeShuffledRDDSorted(rawRDD.asInstanceOf[ShuffledRDD[K, _, _]])
        rawRDD.asUniqueSortedRDD
      }
    }
    def reduceBySortedKey(partitioner: spark.Partitioner, f: (V, V) => V)(
      implicit ck: ClassTag[K], cv: ClassTag[V]) = {

      if (self.isInstanceOf[SortedRDD[K, V]]) {
        self.asInstanceOf[SortedRDD[K, V]].reduceByKey(f)
      } else {
        val rawRDD = self.reduceByKey(partitioner, f)
        makeShuffledRDDSorted(rawRDD.asInstanceOf[ShuffledRDD[K, _, _]])
        rawRDD.asUniqueSortedRDD
      }
    }

    private def makeShuffledRDDSorted[T: Ordering](rawRDD: ShuffledRDD[T, _, _]): Unit = {
      rawRDD.setKeyOrdering(implicitly[Ordering[T]])
    }

    // Returns the RDD unchanged. Fails if the RDD does not have unique
    // keys.
    def assertUniqueKeys(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] =
      self.groupBySortedKey(partitioner)
        .mapValuesWithKeys {
          case (key, id) =>
            assert(id.size == 1,
              s"The ID attribute must contain unique keys. $key appears ${
                id.size
              } times.")
            id.head
        }

  }
}
