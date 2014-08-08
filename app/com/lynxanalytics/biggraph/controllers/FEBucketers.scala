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
    attr: VertexAttribute[T],
    numBuckets: Int): BucketedAttribute[T] = {

    implicit val tt = attr.typeTag
    implicit val mm = metaManager
    implicit val dm = dataManager

    val bucketer = if (typeOf[T] =:= typeOf[String]) {
      val op = ComputeTopValues[String](numBuckets + 1)
      val topVals = op(op.attribute, attr.runtimeSafeCast[String]).result.topValues.value
      StringBucketer(topVals.map(_._1).takeRight(numBuckets), topVals.size == numBuckets + 1)
        .asInstanceOf[Bucketer[T]]
    } else if (typeOf[T] =:= typeOf[Double]) {
      val op = ComputeMinMax[Double]
      val res = op(op.attribute, attr.runtimeSafeCast[Double]).result
      val min = res.min.value
      val max = res.max.value
      DoubleBucketer(min, max, numBuckets).asInstanceOf[Bucketer[T]]
    } else ???

    BucketedAttribute(attr, bucketer)
  }
}
