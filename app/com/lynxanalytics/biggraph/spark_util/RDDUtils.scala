// Assorted utilities for working with RDDs.
package com.lynxanalytics.biggraph.spark_util

import com.esotericsoftware.kryo
import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.graph_api.io.RatioSorter
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
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
      (value, count) => (count / rounder) * rounder
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
    val (valueWeights, unfilteredCount, filteredCount) = sampleWithWeightsAndCounts
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

  // Returns the Partitioner which has more partitions.
  def maxPartitioner(ps: spark.Partitioner*): spark.Partitioner = ps.maxBy(_.numPartitions)

  // Returns an approximation of the number of the rows in rdd. Only use it on RDDs with evenly
  // distributed partitions.
  def countApproxEvenRDD(rdd: RDD[_])(implicit rc: RuntimeContext): Long = {
    if (rdd.partitions.isEmpty) {
      return 0L
    }
    val numPartitions = rdd.partitions.size
    // The sample should not be larger than the number of partitions.
    val numSamplePartitions = rc.numSamplePartitions min numPartitions
    val sampleRatio = numPartitions.toDouble / numSamplePartitions
    (new PartialRDD(rdd, numSamplePartitions).count * sampleRatio).toLong
  }

  // Optionally repartitions the sorted RDD with a "good" partitioner, meaning the number of
  // rows per partition is close to the ideal setting specified in EnityIO. This is expensive,
  // so if the partition sizes are sufficiently close to the ideal no repartitioning happens.
  // This method assumes that the RDD is partitioned evenly.
  def maybeRepartitionForOutput[K: Ordering: ClassTag, V: ClassTag](
    rdd: UniqueSortedRDD[K, V])(implicit rc: RuntimeContext): UniqueSortedRDD[K, V] = {
    val count = countApproxEvenRDD(rdd)
    val desiredNumPartitions = EntityIO.desiredNumPartitions(count)
    if (RatioSorter.ratio(rdd.partitions.size, desiredNumPartitions) < EntityIO.tolerance) {
      rdd
    } else {
      import Implicits._
      val partitioner = new spark.HashPartitioner(desiredNumPartitions)
      rdd.sortUnique(partitioner)
    }
  }
}

object Implicits {
  // Used by RDDUtils.fastNumbered to generate IDs.
  // Args:
  //   parts: The number of partitions.
  //   part: Current partition index.
  //   row: Current row index.
  private[spark_util] def genId(parts: Int, part: Int, row: Int): Long = {
    // HashPartitioner will use nonNegativeMod(id.hashCode, parts) to pick the partition.
    // We generate the ID such that nonNegativeMod(id.hashCode, parts) == part.
    val longId = parts.toLong * row.toLong + part.toLong
    val low = Int.MaxValue % parts
    val high = Int.MinValue % parts
    // Correction for overflows. No correction needed for underflows.
    // low + 1 ≡ high + jump  (mod parts)
    val jump = (low + 1 - high + parts * 2) % parts
    val period = Int.MaxValue.toLong * 2L - jump // Distance between overflows.
    val offset = Int.MaxValue.toLong - jump // Zero is at this point in the period.
    val jumps = (longId + offset) / period
    val jumped = longId + jump * jumps
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
          var uniqueId = pid.toLong - numPartitions
          it.map { value =>
            val randomId = rnd.nextInt.toLong
            uniqueId += numPartitions
            // The ID computed here is guaranteed unique as long as uniqueId fits in one unsigned
            // int. Otherwise it's still unique with large probability.
            ((randomId << 32) ^ uniqueId) -> value
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
      new AlreadySortedRDD(self)
    }
    // Trust that this RDD is partitioned and sorted and has unique keys.
    // Make sure it uses the given partitioner.
    def asUniqueSortedRDD(partitioner: spark.Partitioner)(
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {
      assert(self.partitions.size == partitioner.numPartitions,
        s"Cannot apply partitioner of size ${partitioner.numPartitions}" +
          s" to RDD of size ${self.partitions.size}: $self")
      new AlreadyPartitionedRDD(self, partitioner).asUniqueSortedRDD
    }
    def asUniqueSortedRDD(implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {
      assert(self.partitioner.isDefined,
        s"Cannot cast to unique RDD if there is no partitioner specified")
      new AlreadySortedRDD(self) with UniqueSortedRDD[K, V]
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
      implicit ck: ClassTag[K], cv: ClassTag[V]): UniqueSortedRDD[K, V] = {

      if (self.isInstanceOf[SortedRDD[K, V]]) {
        self.asInstanceOf[SortedRDD[K, V]].reduceByKey(f)
      } else {
        val rawRDD = self.reduceByKey(partitioner, f)
        makeShuffledRDDSorted(rawRDD.asInstanceOf[ShuffledRDD[K, _, _]])
        rawRDD.asUniqueSortedRDD
      }
    }
    def aggregateBySortedKey[U](zeroValue: U, partitioner: spark.Partitioner)(seqOp: (U, V) => U, combOp: (U, U) => U)(
      implicit ck: ClassTag[K], cv: ClassTag[V], cu: ClassTag[U]): UniqueSortedRDD[K, U] = {

      // TODO: efficient implementation for SortedRDDs.
      val rawRDD = self.aggregateByKey(zeroValue, partitioner)(seqOp, combOp)
      makeShuffledRDDSorted(rawRDD.asInstanceOf[ShuffledRDD[K, _, _]])
      rawRDD.asUniqueSortedRDD
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

// An RDD that only has a subset of the partitions from the original RDD.
private[spark_util] class PartialRDD[T: ClassTag](rdd: RDD[T], n: Int) extends RDD[T](rdd) {
  def getPartitions: Array[spark.Partition] = rdd.partitions.take(n)
  override val partitioner = None
  def compute(split: spark.Partition, context: spark.TaskContext) = rdd.compute(split, context)
}
