package com.lynxanalytics.biggraph.graph_util

import java.util.UUID
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.math.Fractional
import scala.math.Numeric

import com.lynxanalytics.biggraph.graph_api._

trait Bucketer[T] extends Serializable {
  def whichBucket(value: T): Int
}

class NumericBucketer[T: Numeric](val min: T, val max: T, numBuckets: Int) extends Bucketer[T] {
  protected val num: Numeric[T] = implicitly[Numeric[T]]
  protected implicit val ops = num.mkNumericOps _

  val bucketSize: T = num.fromInt(((max - min).toLong / numBuckets + 1).toInt)

  def divideByBucketSize(value: T): Int = {
    (value.toLong / bucketSize.toLong).toInt
  }

  def whichBucket(value: T): Int = {
    if (min == max) {
      return 0
    }
    val res = divideByBucketSize(value - min)
    if (res < numBuckets) res else numBuckets - 1
  }

  @transient lazy val bounds: Seq[T] =
    (1 until numBuckets).map(idx => min + num.fromInt(idx) * bucketSize)
}
class FractionalBucketer[T: Fractional](min: T, max: T, numBuckets: Int)
    extends NumericBucketer[T](min, max, numBuckets) {
  private val frac: Fractional[T] = implicitly[Fractional[T]]
  private implicit val fops = frac.mkNumericOps _
  override val bucketSize: T = (max - min) / num.fromInt(numBuckets)
  override def divideByBucketSize(value: T): Int = (value / bucketSize).toInt
}

trait VertexAttributeBucketing[T] {
  val attribute: VertexAttribute[T]
  val bucketLabels: Seq[String]
  val bucketer: Bucketer[T]
}

class NumericVertexBucketing[T](val attribute: VertexAttribute[T],
                                val bucketer: NumericBucketer[T]) {
  val bucketLabels = (Seq(bucketer.min) ++ bucketer.bounds.dropRight(1)).zip(bucketer.bounds)
    .map { case (lowerBound, upperBound) => s"[$lowerBound, $upperBound)" } ++
    Seq("[%s, %s]".format(bucketer.bounds.last, bucketer.max))
}
