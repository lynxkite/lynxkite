// Assorted utilities for working with RDDs.
package com.lynxanalytics.biggraph.spark_util

import com.esotericsoftware.kryo
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable
import scala.reflect._
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._

// A container for storing ID counts per bucket and a sample.
class IDBuckets[T] extends Serializable {
  val counts = mutable.Map[T, Long]().withDefaultValue(0)
  var sample = mutable.Map[ID, T]() // May be null!
  def add(id: ID, t: T) = {
    counts(t) += 1
    addSample(id, t)
  }
  def absorb(b: IDBuckets[T]) = {
    for ((k, v) <- b.counts) {
      counts(k) += v
    }
    if (b.sample == null) {
      sample = null
    } else for ((id, t) <- b.sample) {
      addSample(id, t)
    }
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

object RDDUtils {
  val threadLocalKryo = new ThreadLocal[kryo.Kryo] {
    override def initialValue() = BigGraphSparkContext.createKryo()
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
    threadLocalKryo.get.writeClassAndObject(oos, obj)
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
    full: SortedRDD[ID, _], restricted: SortedRDD[ID, T]): SortedRDD[ID, (T, Int)] =
    new BiDerivedSortedRDD(
      full,
      restricted,
      (fullRDD: SortedRDD[ID, _], restrictedRDD: SortedRDD[ID, T]) =>
        fullRDD.zipPartitions(restrictedRDD, true) { (fit, rit) =>
          new Iterator[(ID, (T, Int))] {
            def hasNext = rit.hasNext
            def next() = {
              val nxt = rit.next
              var c = 1
              while (fit.next._1 < nxt._1) c += 1
              (nxt._1, (nxt._2, c))
            }
          }
        })

  def estimateValueCounts[T](
    fullRDD: SortedRDD[ID, _],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int): IDBuckets[T] = {

    val dataUsed = data.takeFirstNValuesOrSo(requiredPositiveSamples)
    val withCounts = unfilteredCounts(fullRDD, dataUsed)
    val (valueBuckets, unfilteredCount, filteredCount) = withCounts
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
    return valueBuckets
  }

  def estimateValueWeights[T](
    fullRDD: SortedRDD[ID, _],
    weightsRDD: SortedRDD[ID, Double],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int): Map[T, Double] = {

    val dataUsed = data.takeFirstNValuesOrSo(requiredPositiveSamples)
    val withWeightsAndCounts = unfilteredCounts(fullRDD, dataUsed.sortedJoin(weightsRDD))
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
    def randomNumbered(numPartitions: Int = self.partitions.size): SortedRDD[ID, T] = {
      val partitioner = new spark.HashPartitioner(numPartitions)
      randomNumbered(partitioner)
    }

    def randomNumbered(partitioner: spark.Partitioner): SortedRDD[ID, T] = {
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
      }).toSortedRDD(partitioner)
    }

    // Cheap method to force an RDD calculation
    def calculate() = self.foreach(_ => ())

    def printDetails(indent: Int = 0): Unit = {
      println(" " * indent + s"- $self (${self.partitions.size} partitions)")
      for (dep <- self.dependencies) {
        dep.rdd.printDetails(indent + 1)
      }
    }
  }

  implicit class PairRDDUtils[K: Ordering, V](self: RDD[(K, V)]) extends Serializable {
    // Sorts each partition of the RDD in isolation.
    def toSortedRDD = SortedRDD.fromUnsorted(self)
    def toSortedRDD(partitioner: spark.Partitioner)(implicit ck: ClassTag[K], cv: ClassTag[V]) =
      SortedRDD.fromUnsorted(self.partitionBy(partitioner))
    def groupBySortedKey(partitioner: spark.Partitioner)(implicit ck: ClassTag[K], cv: ClassTag[V]) =
      SortedRDD.fromUnsorted(self.groupByKey(partitioner))
    def reduceBySortedKey(partitioner: spark.Partitioner, f: (V, V) => V)(implicit ck: ClassTag[K], cv: ClassTag[V]) =
      SortedRDD.fromUnsorted(self.reduceByKey(partitioner, f))
  }
}
