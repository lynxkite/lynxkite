package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.spark_util.RDDUtils
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd

import attributes.DenseAttributes


object GraphIO {
  def loadFromObjectFile(sc: spark.SparkContext, pathPrefix: String): (VertexRDD, EdgeRDD) = {
    val vertices = RDDUtils.objectFile[(graphx.VertexId, DenseAttributes)](
        sc, pathPrefix + ".vertices")
    val edges = RDDUtils.objectFile[graphx.Edge[DenseAttributes]](
        sc, pathPrefix + ".edges")
    return (vertices, edges)
  }

  def getSerializedSize(obj: Any): Int = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(obj)
    objectStream.close()
    return buffer.size
  }

  def coalesceToPartitionSize[T](
    source: rdd.RDD[T],
    desiredSizeInBytes: Int)
      : rdd.RDD[T] = {
    val (firstElem, firstPartSize) = source.glom.map(arr => {
      val size = arr.size
      if (size > 0) {
        (Some(arr(0)), size)
      } else {
        (None, 0)
      }
    }).first
    if (firstPartSize == 0) {
      return source
    }
    val serializedSizeEstimate =
      getSerializedSize(firstElem.get) *
        firstPartSize *
        source.partitions.size
    val desiredParitions = serializedSizeEstimate / desiredSizeInBytes
    if (desiredParitions >= source.partitions.size) source
    else source.coalesce(
      if (desiredParitions < 1) 1 else desiredParitions.toInt)
  }


  def saveAsObjectFile(data: GraphData, pathPrefix: String) {
    coalesceToPartitionSize(
      data.vertices,
      64 * 1024 * 1024).saveAsObjectFile(pathPrefix + ".vertices")
    coalesceToPartitionSize(
      data.edges.map(_.copy()),
      64 * 1024 * 1024).saveAsObjectFile(pathPrefix + ".edges")
  }
}
