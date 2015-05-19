// Classes for sorting attribute values into buckets.
package com.lynxanalytics.biggraph.graph_util

import scala.math.Fractional
import scala.math.Numeric

import com.lynxanalytics.biggraph.graph_api._

trait Bucketer[-T] extends Serializable with ToJson {
  val numBuckets: Int
  def whichBucket(value: T): Option[Int]
  def bucketLabels: Seq[String]
  def bucketFilters: Seq[String] = Seq() // Filter string equivalent of the bucket. (Optional.)
  def labelType: String // "bucket" or "between".
  def isEmpty = false
  final def nonEmpty = !isEmpty
}

object EmptyBucketer extends FromJson[EmptyBucketer] {
  def fromJson(j: JsValue) = EmptyBucketer()
}
case class EmptyBucketer() extends Bucketer[Nothing] {
  val numBuckets = 1
  def whichBucket(value: Nothing) = ???
  def bucketLabels: Seq[String] = Seq("")
  def labelType = ""
  override def isEmpty = true
}

abstract class EnumBucketer[T](options: Seq[T], hasOther: Boolean) extends Bucketer[T] {
  val mapToIdx = options.zipWithIndex.toMap
  val numBuckets = if (hasOther) options.size + 1 else options.size
  def whichBucket(value: T) = mapToIdx.get(value).orElse(if (hasOther) Some(options.size) else None)
  val optionLabels = options.map(_.toString)
  val bucketLabels = if (hasOther) optionLabels :+ "Other" else optionLabels
}

abstract class NumericBucketer[T: Numeric](
  min: T, max: T, nb: Int)
    extends Bucketer[T] {
  protected val num: Numeric[T] = implicitly[Numeric[T]]
  import num.mkNumericOps
  import num.mkOrderingOps

  val bucketSize: T = num.fromInt(((max - min).toLong / nb + 1).toInt)

  def whichBucket(value: T): Option[Int] = {
    val lessThan = bounds.indexWhere(value < _)
    if (lessThan == -1) Some(nb - 1) else Some(lessThan)
  }

  @transient lazy val bounds: Seq[T] =
    (1 until nb).map(idx => min + num.fromInt(idx) * bucketSize)

  def bucketLabels: Seq[String] = {
    var labels = min +: bounds :+ max
    labels.map(_.toString)
  }
}

abstract class FractionalBucketer[T: Fractional](min: T, max: T, nb: Int)
    extends NumericBucketer[T](min, max, nb) {
  private val frac: Fractional[T] = implicitly[Fractional[T]]
  private implicit val fops = frac.mkNumericOps _
  override val bucketSize: T = (max - min) / num.fromInt(nb)
}

object StringBucketer extends FromJson[StringBucketer] {
  def fromJson(j: JsValue) =
    StringBucketer((j \ "options").as[Seq[String]], (j \ "hasOther").as[Boolean])
}
case class StringBucketer(options: Seq[String], hasOther: Boolean)
    extends EnumBucketer[String](options, hasOther) {
  val labelType = "bucket"
  override def toJson = Json.obj("options" -> options, "hasOther" -> hasOther)
  override def bucketFilters = {
    if (hasOther) {
      val otherFilter = "!" + optionLabels.mkString(",")
      optionLabels :+ otherFilter
    } else optionLabels
  }
}

abstract class DoubleBucketer(min: Double, max: Double, numBuckets: Int)
    extends FractionalBucketer[Double](min, max, numBuckets) {
  val labelType = "between"

  def fmt(num: Double, decimals: Int): String =
    s"%.${decimals}f".format(num)
  def fmt(nums: Seq[Double], decimals: Int): Seq[String] =
    nums.map(fmt(_, decimals))
  override def bucketLabels: Seq[String] = {
    val labels = min +: bounds :+ max
    if (min == max) {
      fmt(labels, 2)
    } else {
      val maxDecimals = 10
      // Use enough decimals that all the labels are different.
      val decimals = (0 to maxDecimals).find { decimals =>
        fmt(labels, decimals).toSet.size == labels.size
      }
      fmt(labels, decimals.getOrElse(2))
    }
  }

  override def bucketFilters = {
    val ls = bucketLabels.drop(1).dropRight(1)
    if (ls.size < 1) Seq() else {
      val first = "<" + ls.head
      val last = ">=" + ls.last
      val middle = if (ls.size < 2) Seq() else {
        ls.sliding(2).map { ab =>
          val a = ab(0)
          val b = ab(1)
          s"[$a,$b)"
        }.toSeq
      }
      first +: middle :+ last
    }
  }
}

object DoubleLinearBucketer extends FromJson[DoubleLinearBucketer] {
  def fromJson(j: JsValue) =
    DoubleLinearBucketer((j \ "min").as[Double], (j \ "max").as[Double], (j \ "numBuckets").as[Int])
}
case class DoubleLinearBucketer(min: Double, max: Double, numBuckets: Int)
    extends DoubleBucketer(min, max, numBuckets) {
  override def toJson = Json.obj(
    "min" -> min, "max" -> max, "numBuckets" -> numBuckets)
}

object DoubleLogBucketer extends FromJson[DoubleLogBucketer] {
  def fromJson(j: JsValue) =
    DoubleLogBucketer((j \ "min").as[Double], (j \ "max").as[Double], (j \ "numBuckets").as[Int])
}
case class DoubleLogBucketer(min: Double, max: Double, numBuckets: Int)
    extends DoubleBucketer(min, max, numBuckets) {
  assert(min > 0, s"Cannot take the logarithm of $min.")
  override def toJson = Json.obj(
    "min" -> min, "max" -> max, "numBuckets" -> numBuckets)
  @transient override lazy val bounds: Seq[Double] = {
    val logMin = math.log(min)
    val logMax = math.log(max)
    val logBucketSize = (logMax - logMin) / numBuckets
    val logBounds = (1 until numBuckets).map(idx => logMin + idx * logBucketSize)
    logBounds.map(x => math.exp(x))
  }
}

object LongBucketer extends FromJson[LongBucketer] {
  def fromJson(j: JsValue) =
    LongBucketer((j \ "min").as[Long], (j \ "max").as[Long], (j \ "numBuckets").as[Int])
}
case class LongBucketer(min: Long, max: Long, numBuckets: Int)
    extends NumericBucketer[Long](min, max, numBuckets) {
  override def toJson = Json.obj(
    "min" -> min, "max" -> max, "numBuckets" -> numBuckets)
  val labelType = if ((max - min) / numBuckets == 0) "bucket" else "between"
  override def bucketLabels: Seq[String] = {
    if ((max - min) / numBuckets == 0)
      super.bucketLabels.init
    else
      super.bucketLabels
  }
}

abstract class MapBucketer[T](toBucket: Map[T, Int]) extends Bucketer[T] {
  val numBuckets = if (toBucket.isEmpty) 0 else toBucket.values.max + 1
  def whichBucket(value: T) = toBucket.get(value)
  def bucketLabels = ???
  def labelType = ???
}

object IDMapBucketer extends FromJson[IDMapBucketer] {
  def fromJson(j: JsValue) =
    IDMapBucketer((j \ "toBucket").as[Map[String, String]].map { case (k, v) => k.toLong -> v.toInt })
}
case class IDMapBucketer(toBucket: Map[ID, Int]) extends MapBucketer[ID](toBucket) {
  override def toJson = Json.obj(
    "toBucket" -> toBucket.map { case (k, v) => k.toString -> v.toString })
}
