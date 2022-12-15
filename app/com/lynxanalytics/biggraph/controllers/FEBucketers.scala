// Creates the right Bucketer for an attribute based on the AxisOptions.
package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.graph_api.Scripting._

object FEBucketers {
  def bucketedAttribute[T](
      metaManager: MetaGraphManager,
      dataManager: DataManager,
      attr: Attribute[T],
      numBuckets: Int,
      axisOptions: AxisOptions): BucketedAttribute[T] = {

    implicit val tt = attr.typeTag
    implicit val mm = metaManager
    implicit val dm = dataManager

    val bucketer =
      if (typeOf[T] =:= typeOf[String]) {
        val op = ComputeTopValues[String](numBuckets + 1, 10000)
        val topVals = op(op.attribute, attr.runtimeSafeCast[String]).result.topValues.value
        // Sort by decreasing counts, break ties alphabetically.
        val sorted = topVals.sortBy(_._1).sortBy(-_._2).map(_._1)
        val hasOther = sorted.size == numBuckets + 1
        (if (hasOther) StringBucketer(sorted.take(numBuckets - 1), true)
         else StringBucketer(sorted, false))
          .asInstanceOf[Bucketer[T]]
      } else if (typeOf[T] =:= typeOf[Double]) {
        val stats = {
          val op = ComputeMinMaxMinPositiveDouble()
          op(op.attribute, attr.runtimeSafeCast[Double]).result
        }
        val min = stats.min.value.getOrElse(0.0)
        val max = stats.max.value.getOrElse(0.0)
        val minPositive = stats.minPositive.value
        if (axisOptions.logarithmic)
          DoubleLogBucketer(min, max, minPositive, numBuckets).asInstanceOf[Bucketer[T]]
        else
          DoubleLinearBucketer(min, max, numBuckets).asInstanceOf[Bucketer[T]]
      } else ???

    BucketedAttribute(attr, bucketer)
  }
}
