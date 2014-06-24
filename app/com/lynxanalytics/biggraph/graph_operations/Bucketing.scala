package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class VertexBucketGrid(xSize: Int,
                            ySize: Int,
                            xMin: Double,
                            xMax: Double,
                            yMin: Double,
                            yMax: Double) extends MetaGraphOperation {
  assert(xSize >= 1)
  assert(ySize >= 1)
  def signature = {
    var sig = newSignature
      .inputVertexSet('vertices)
    if (xSize > 1) {
      sig = sig.inputVertexAttribute[Double]('xAttribute, 'vertices)
    }
    if (ySize > 1) {
      sig = sig.inputVertexAttribute[Double]('yAttribute, 'vertices)
    }
    sig
      .outputScalar[Map[(Int, Int), Int]]('bucketSizes)
      .outputVertexAttribute[Int]('xBuckets)
      .outputVertexAttribute[Int]('yBuckets)
  }

  val xBucketLabels = if (xSize == 1) {
    Seq("")
  } else {
    NumericBucketer.bucketLabels(new FractionalBucketer[Double](xMin, xMax, xSize))
  }

  val yBucketLabels = if (ySize == 1) {
    Seq("")
  } else {
    NumericBucketer.bucketLabels(new FractionalBucketer[Double](yMin, yMax, ySize))
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vertices = inputs.vertexSets('vertices).rdd
    val xBuckets = if (xSize == 1) {
      vertices.mapValues(_ => 0)
    } else {
      val bucketer = new FractionalBucketer[Double](xMin, xMax, xSize)
      val xAttr = inputs.vertexAttributes('xAttribute).runtimeSafeCast[Double].rdd
      vertices.join(xAttr).mapValues { case (_, value) => bucketer.whichBucket(value) }
    }
    val yBuckets = if (ySize == 1) {
      vertices.mapValues(_ => 0)
    } else {
      val bucketer = new FractionalBucketer[Double](yMin, yMax, ySize)
      val yAttr = inputs.vertexAttributes('yAttribute).runtimeSafeCast[Double].rdd
      vertices.join(yAttr).mapValues { case (_, value) => bucketer.whichBucket(value) }
    }
    outputs.putVertexAttribute('xBuckets, xBuckets)
    outputs.putVertexAttribute('yBuckets, yBuckets)
    outputs.putScalar('bucketSizes,
      xBuckets.join(yBuckets)
        .map { case (vid, (xB, yB)) => ((xB, yB), 1) }
        .reduceByKey(_ + _)
        .collect
        .toMap)
  }
}
