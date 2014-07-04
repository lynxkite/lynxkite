package com.lynxanalytics.biggraph.graph_util

import scala.reflect.runtime.universe._
import scala.math.Fractional
import scala.math.Numeric

import com.lynxanalytics.biggraph.graph_api._

trait Bucketer[T] extends Serializable {
  def tt: TypeTag[T]
  val numBuckets: Int
  def whichBucket(value: T): Int
  def bucketLabels: Seq[String]
}

case class EmptyBucketer() extends Bucketer[Nothing] {
  def tt: TypeTag[Nothing] = ???
  val numBuckets = 1
  def whichBucket(value: Nothing) = ???
  def bucketLabels: Seq[String] = Seq("")
}

abstract class EnumBucketer[T](options: Seq[T], hasOther: Boolean) extends Bucketer[T] {
  val mapToIdx = options.zipWithIndex.toMap
  val numBuckets = if (hasOther) options.size + 1 else options.size
  def whichBucket(value: T) = mapToIdx.getOrElse(value, if (hasOther) options.size else ???)
  val optionLabels = options.map(_.toString)
  val bucketLabels = if (hasOther) optionLabels :+ "Other" else optionLabels
}

abstract class NumericBucketer[T: Numeric](
  min: T, max: T, nb: Int)
    extends Bucketer[T] {
  protected val num: Numeric[T] = implicitly[Numeric[T]]
  protected implicit val ops = num.mkNumericOps _

  val bucketSize: T = num.fromInt(((max - min).toLong / nb + 1).toInt)

  def divideByBucketSize(value: T): Int = {
    (value.toLong / bucketSize.toLong).toInt
  }

  def whichBucket(value: T): Int = {
    if (min == max) {
      return 0
    }
    val res = divideByBucketSize(value - min)
    if (res < nb) res else nb - 1
  }

  @transient lazy val bounds: Seq[T] =
    (1 until nb).map(idx => min + num.fromInt(idx) * bucketSize)

  implicit class Formatter(val stringContext: StringContext) {
    def fmt(args: Any*) = {
      val formatted = args.map(a => format(a.asInstanceOf[T]))
      stringContext.s(formatted: _*)
    }

    private def format(n: T) = {
      if (num.equiv(n, num.zero)) {
        "0"
      } else if (num.lt(n, num.one)) {
        f"${n.toDouble}%.2f"
      } else {
        n.toLong.toString
      }
    }
  }

  def bucketLabels: Seq[String] = {
    val normalLabels = (Seq(min) ++ bounds.dropRight(1)).zip(bounds).map {
      case (lowerBound, upperBound) => fmt"[$lowerBound, $upperBound)"
    }
    val lastLabel = fmt"[${bounds.last}, $max]"
    normalLabels :+ lastLabel
  }
}

abstract class FractionalBucketer[T: Fractional](min: T, max: T, nb: Int)
    extends NumericBucketer[T](min, max, nb) {
  private val frac: Fractional[T] = implicitly[Fractional[T]]
  private implicit val fops = frac.mkNumericOps _
  override val bucketSize: T = (max - min) / num.fromInt(nb)
  override def divideByBucketSize(value: T): Int = (value / bucketSize).toInt
}

case class StringBucketer(options: Seq[String], hasOther: Boolean)
    extends EnumBucketer[String](options, hasOther) {
  @transient lazy val tt = typeTag[String]
}
case class DoubleBucketer(min: Double, max: Double, numBuckets: Int)
    extends FractionalBucketer[Double](min, max, numBuckets) {
  @transient lazy val tt = typeTag[Double]
}
case class LongBucketer(min: Long, max: Long, numBuckets: Int)
    extends NumericBucketer[Long](min, max, numBuckets) {
  @transient lazy val tt = typeTag[Long]
}
