// Classes for sorting attribute values into buckets.
package com.lynxanalytics.biggraph.graph_util

import scala.math.Fractional
import scala.math.Numeric

import com.lynxanalytics.biggraph.graph_api._

trait Bucketer[-T] extends Serializable with ToJson {
  def numBuckets: Int = {
    labelType match {
      case "bucket" => bucketLabels.size
      case "between" => bucketLabels.size - 1
    }
  }
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
  def whichBucket(value: Nothing) = ???
  def bucketLabels: Seq[String] = Seq("")
  def labelType = "bucket"
  override def isEmpty = true
}

abstract class EnumBucketer[T](options: Seq[T], hasOther: Boolean) extends Bucketer[T] {
  val mapToIdx = options.zipWithIndex.toMap
  def whichBucket(value: T) = mapToIdx.get(value).orElse(if (hasOther) Some(options.size) else None)
  val optionLabels = options.map(_.toString)
  val bucketLabels = if (hasOther) optionLabels :+ "Other" else optionLabels
}

abstract class NumericBucketer[T: Numeric](
  min: T, max: T, desiredBuckets: Int)
    extends Bucketer[T] {
  protected val num: Numeric[T] = implicitly[Numeric[T]]
  import num.mkNumericOps
  import num.mkOrderingOps

  // Helper values for the default implementation of bounds.
  protected val actualBuckets = if (max == min) 1 else desiredBuckets
  protected val bucketSize: T = num.fromInt(((max - min).toLong / actualBuckets + 1).toInt)

  def whichBucket(value: T): Option[Int] = {
    val lessThan = bounds.indexWhere(value < _)
    if (lessThan == -1) Some(bounds.size) else Some(lessThan)
  }

  // Intermediate boundaries, not including min and max.
  @transient lazy val bounds: Seq[T] =
    (1 until actualBuckets).map(idx => min + num.fromInt(idx) * bucketSize)

  def bucketLabels: Seq[String] = {
    val labels = min +: bounds :+ max
    labels.map(_.toString)
  }
}

abstract class FractionalBucketer[T: Fractional](min: T, max: T, desiredBuckets: Int)
    extends NumericBucketer[T](min, max, desiredBuckets) {
  private val frac: Fractional[T] = implicitly[Fractional[T]]
  private implicit val fops = frac.mkNumericOps _
  override val bucketSize: T = (max - min) / num.fromInt(actualBuckets)
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

abstract class DoubleBucketer(min: Double, max: Double, desiredBuckets: Int)
    extends FractionalBucketer[Double](min, max, desiredBuckets) {
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
      fmt(labels, decimals.getOrElse(maxDecimals) + 1)
    }
  }

  override def bucketFilters = {
    if (bounds.isEmpty) Seq("")
    else {
      val first = s"<${bounds.head}"
      val last = s">=${bounds.last}"
      val middles = bounds
        .sliding(2)
        // To deal with the sliding API that returns a shorter window if the whole seq
        // is shorter then the requested sliding window. Idiots.
        .filter(_.size == 2)
        .map { case Seq(from, to) => s"[$from,$to)" }
        .toSeq
      first +: middles :+ last
    }
  }

}

object DoubleLinearBucketer extends FromJson[DoubleLinearBucketer] {
  def fromJson(j: JsValue) =
    DoubleLinearBucketer((j \ "min").as[Double], (j \ "max").as[Double], (j \ "numBuckets").as[Int])
}
case class DoubleLinearBucketer(min: Double, max: Double, desiredBuckets: Int)
    extends DoubleBucketer(min, max, desiredBuckets) {
  override def toJson = Json.obj(
    "min" -> min, "max" -> max, "numBuckets" -> desiredBuckets)
}

object DoubleLogBucketer extends FromJson[DoubleLogBucketer] {
  def fromJson(j: JsValue) = {
    val min = (j \ "min").as[Double]
    val minPositive = (j \ "minPositive").asOpt[Double]
    val version = (j \ "version").asOpt[Int].getOrElse(0)
    DoubleLogBucketer(
      min,
      (j \ "max").as[Double],
      if (version >= 1) minPositive else Some(min), // Compatibility.
      (j \ "numBuckets").as[Int])
  }
}
case class DoubleLogBucketer(
  min: Double, max: Double, minPositive: Option[Double], desiredBuckets: Int)
    extends DoubleBucketer(min, max, desiredBuckets) {
  for (minPositive <- minPositive) {
    assert(minPositive > 0, s"Cannot take the logarithm of $minPositive.")
  }
  override def toJson = {
    val original = Json.obj("min" -> min, "max" -> max, "numBuckets" -> desiredBuckets)
    minPositive match {
      case Some(minPositive) =>
        if (minPositive == min) original // Compatibility.
        else original ++ Json.obj("version" -> 1, "minPositive" -> minPositive)
      case None =>
        original ++ Json.obj("version" -> 1)
    }
  }
  @transient override lazy val bounds: Seq[Double] = {
    minPositive match {
      case Some(minPositive) =>
        val positiveBounds = if (minPositive == max) Seq() else {
          val logMin = math.log(minPositive)
          val logMax = math.log(max)
          val logBucketSize = (logMax - logMin) / desiredBuckets
          val logBounds = (1 until desiredBuckets).map(idx => logMin + idx * logBucketSize)
          logBounds.map(x => math.exp(x))
        }
        if (min == minPositive) positiveBounds
        else minPositive +: positiveBounds // Extra bucket for non-positive values.
      // No positive values, put everything in one bucket.
      case None => Seq()
    }
  }
}

object LongBucketer extends FromJson[LongBucketer] {
  def fromJson(j: JsValue) =
    LongBucketer((j \ "min").as[Long], (j \ "max").as[Long], (j \ "numBuckets").as[Int])
}
case class LongBucketer(min: Long, max: Long, desiredBuckets: Int)
    extends NumericBucketer[Long](min, max, desiredBuckets) {
  override def toJson = Json.obj(
    "min" -> min, "max" -> max, "numBuckets" -> desiredBuckets)
  val labelType = if ((max - min) / desiredBuckets == 0) "bucket" else "between"
  override def bucketLabels: Seq[String] = {
    if ((max - min) / desiredBuckets == 0)
      super.bucketLabels.init
    else
      super.bucketLabels
  }
}

abstract class MapBucketer[T](toBucket: Map[T, Int]) extends Bucketer[T] {
  override def numBuckets = if (toBucket.isEmpty) 0 else toBucket.values.max + 1
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
