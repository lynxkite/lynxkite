package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util._

object FEBucketers {
  def bucketer[T](
    metaManager: MetaGraphManager,
    dataManager: DataManager,
    attr: VertexAttribute[T],
    numBuckets: Int): Bucketer[T] = {

    implicit val tt = attr.typeTag
    if (typeOf[T] =:= typeOf[String]) {
      val attribute = attr.runtimeSafeCast[String]
      val topVals = ComputeTopValues(metaManager, dataManager, attribute, numBuckets + 1)
      StringBucketer(topVals.map(_._1).takeRight(numBuckets), topVals.size == numBuckets + 1)
        .asInstanceOf[Bucketer[T]]
    } else if (typeOf[T] =:= typeOf[Double]) {
      val attribute = attr.runtimeSafeCast[Double]
      val (min, max) = ComputeMinMax(metaManager, dataManager, attribute)
      DoubleBucketer(min, max, numBuckets).asInstanceOf[Bucketer[T]]
    } else ???
  }
}
