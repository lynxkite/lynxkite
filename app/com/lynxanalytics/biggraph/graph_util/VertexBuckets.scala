package com.lynxanalytics.biggraph.graph_util

import java.util.UUID
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.math.Numeric

import com.lynxanalytics.biggraph.graph_api._

trait Bucketer[T] extends Serializable {
  def whichBucket(value: T): Int
}
class NumericBucketer[T: Numeric](min: T, max: T, numBuckets: Int) extends Bucketer[T] {
  val num = implicitly[Numeric[T]]
  def whichBucket(value: T): Int = {
    val res = (numBuckets * (num.toDouble(value) - num.toDouble(min))
      / (num.toDouble(max) - num.toDouble(min))).toInt
    if (res < numBuckets) res else numBuckets - 1
  }
}

trait AttributeBucketing[T] {
  val attribute: VertexAttribute[T]
  val bucketLabels: Seq[String]
  val bucketer: Bucketer[T]
}
