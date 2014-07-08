package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class VertexBucketGrid[S, T](xBucketer: Bucketer[S],
                                  yBucketer: Bucketer[T]) extends MetaGraphOperation {
  def signature = {
    var sig = newSignature
      .inputVertexSet('vertices)
    if (xBucketer.numBuckets > 1) {
      implicit val tt = xBucketer.tt
      sig = sig.inputVertexAttribute[S]('xAttribute, 'vertices)
    }
    if (yBucketer.numBuckets > 1) {
      implicit val tt = yBucketer.tt
      sig = sig.inputVertexAttribute[T]('yAttribute, 'vertices)
    }
    sig
      .outputScalar[Map[(Int, Int), Int]]('bucketSizes)
      .outputVertexAttribute[Int]('xBuckets, 'vertices)
      .outputVertexAttribute[Int]('yBuckets, 'vertices)
      .outputVertexAttribute[Int]('feIdxs, 'vertices)
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vertices = inputs.vertexSets('vertices).rdd
    val xBuckets = if (xBucketer.numBuckets == 1) {
      vertices.mapValues(_ => 0)
    } else {
      implicit val tt = xBucketer.tt
      val xAttr = inputs.vertexAttributes('xAttribute).runtimeSafeCast[S].rdd
      vertices.join(xAttr).mapValues { case (_, value) => xBucketer.whichBucket(value) }
    }
    val yBuckets = if (yBucketer.numBuckets == 1) {
      vertices.mapValues(_ => 0)
    } else {
      implicit val tt = yBucketer.tt
      val yAttr = inputs.vertexAttributes('yAttribute).runtimeSafeCast[T].rdd
      vertices.join(yAttr).mapValues { case (_, value) => yBucketer.whichBucket(value) }
    }
    outputs.putVertexAttribute('xBuckets, xBuckets)
    outputs.putVertexAttribute('yBuckets, yBuckets)
    outputs.putVertexAttribute(
      'feIdxs,
      xBuckets.join(yBuckets).mapValues { case (x, y) => x * yBucketer.numBuckets + y })
    outputs.putScalar('bucketSizes,
      xBuckets.join(yBuckets)
        .map { case (vid, (xB, yB)) => ((xB, yB), 1) }
        .reduceByKey(_ + _)
        .collect
        .toMap)
  }
}
