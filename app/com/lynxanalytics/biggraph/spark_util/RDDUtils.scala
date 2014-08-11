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

object RDDUtils {
  val threadLocalKryo = new ThreadLocal[kryo.Kryo] {
    override def initialValue(): kryo.Kryo = {
      val myKryo = new kryo.Kryo()
      myKryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
      new BigGraphKryoRegistrator().registerClasses(myKryo)
      myKryo
    }
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
   * interger for each key. This interger is the number of keys found in fullRDD between
   * the actual key (inclusive) and the previous key in restricted (exclusive). Summing up that
   * integer on a per-partition prefix gives an estimate of how much of the original, full RDD
   * we had to process to get the filtered sample we needed. This is necessary to be able to
   * estimate totals from the filtered numbers.
   */
  private def unfilteredCounts[T](
    fullRDD: SortedRDD[ID, _], restrictedRDD: SortedRDD[ID, T]): SortedRDD[ID, (T, Int)] = {
    val res = fullRDD.zipPartitions(restrictedRDD, true) { (fit, rit) =>
      val bfit = fit.buffered
      val brit = rit.buffered
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
    new SortedRDD(res)
  }

  def estimateValueCounts[T](
    fullRDD: SortedRDD[ID, _],
    data: SortedRDD[ID, T],
    totalVertexCount: Long,
    requiredPositiveSamples: Int): Map[T, Int] = {

    val dataUsed = data.takeFirstNValuesOrSo(requiredPositiveSamples)
    val withCounts = unfilteredCounts(fullRDD, dataUsed)
    val (valueCounts, unfilteredCount) = withCounts
      .values
      .aggregate((mutable.Map[T, Int](), 0))(
        {
          case ((map, uct), (key, uc)) =>
            incrementMap(map, key)
            (map, uct + uc)
        },
        {
          case ((map1, uct1), (map2, uct2)) =>
            map2.foreach { case (k, v) => incrementMap(map1, k, v) }
            (map1, uct1 + uct2)
        })
    val multiplier = totalVertexCount * 1.0 / unfilteredCount
    valueCounts.toMap.mapValues(c => math.round(multiplier * c).toInt)
  }

  def incrementMap[K](map: mutable.Map[K, Int], key: K, increment: Int = 1): Unit = {
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
    // The returned RDD will be partitioned by the partitioner of the input (if it is
    // a HashPartitioner) or by a new HashPartitioner.
    def fastNumbered: RDD[(ID, T)] = {
      val numPartitions = self.partitions.size
      val partitioner = self.partitioner.collect {
        case p: spark.HashPartitioner => p
      }.getOrElse(new spark.HashPartitioner(numPartitions))
      fastNumbered(partitioner)
    }

    // Adds unique ID numbers to rows of an RDD as a transformation.
    // The returned RDD will be partitioned by the given partitioner.
    def fastNumbered(partitioner: spark.Partitioner): RDD[(ID, T)] = {
      require(partitioner.isInstanceOf[spark.HashPartitioner], s"Need HashPartitioner, got: $partitioner")
      val numPartitions = partitioner.numPartitions
      // Need to repartition before adding the IDs if we are going to change the partition count.
      val rightPartitions = if (numPartitions == self.partitions.size) self else self.repartition(numPartitions)
      // Add IDs.
      val withIDs = rightPartitions.mapPartitionsWithIndex({
        case (pid, it) => it.zipWithIndex.map {
          case (el, fID) => genID(numPartitions, pid, fID) -> el
        }
      }, preservesPartitioning = true)
      // If the RDD was already partitioned correctly, we can skip the (pointless) shuffle.
      if (withIDs.partitioner == Some(partitioner)) withIDs
      else withIDs.partitionBy(partitioner)
    }

    // Adds unique ID numbers to rows of an RDD as a transformation.
    // A new HashPartitioner will shuffle data randomly among partitions
    // in order to provide unbiased data for sampling SortedRDDs.
    def randomNumbered(numPartitions: Int = self.partitions.size): SortedRDD[ID, T] = {
      val partitioner = new spark.HashPartitioner(numPartitions)

      // generate a random id for the hash
      val randomPartitioned = self.mapPartitionsWithIndex({
        case (pid, it) =>
          val rnd = new scala.util.Random(pid)
          it.map(rnd.nextLong -> _)
      }).partitionBy(partitioner)

      val shuffled = randomPartitioned.mapPartitionsWithIndex({
        case (pid, xs) =>
          val rnd = new scala.util.Random(pid)
          rnd.shuffle(xs)
      }, preservesPartitioning = true)

      // generate unique id, throw away previous random id
      shuffled.fastNumbered(partitioner).mapValues(_._2).toSortedRDD
    }

    // Cheap method to force an RDD calculation
    def calculate() = self.foreach(_ => ())
  }

  implicit class PairRDDUtils[K: Ordering, V](self: RDD[(K, V)]) extends Serializable {
    // Sorts each partition of the RDD in isolation.
    def toSortedRDD = SortedRDD.fromUnsorted(self)
    def toSortedRDD(partitioner: spark.Partitioner)(implicit ck: ClassTag[K], cv: ClassTag[V]) =
      SortedRDD.fromUnsorted(self.partitionBy(partitioner))
    def groupBySortedKey(partitioner: spark.Partitioner)(implicit ck: ClassTag[K], cv: ClassTag[V]) =
      SortedRDD.fromUnsorted(self.groupByKey(partitioner))
    def asSortedRDD = SortedRDD.fromSorted(self)
  }
}
