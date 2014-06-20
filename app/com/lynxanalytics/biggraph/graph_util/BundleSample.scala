package com.lynxanalytics.biggraph.graph_util

import java.util.UUID
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd
import scala.collection.mutable
import scala.reflect.ClassTag

import com.lynxanalytics.biggraph.graph_api._

class EdgeBundleSample(edgeBundle: EdgeBundle,
                       targetSampleSize: Int,
                       dataManager: DataManager) {
  val edgeBundleData = dataManager.get(edgeBundle)
  val originalRDD = edgeBundleData.rdd
  // TODO(xandrew): once we have cached statistics somehow take this from there.
  val originalSize = originalRDD.count
  val sample =
    if (originalSize > targetSampleSize) originalRDD.sample(
      withReplacement = false,
      fraction = targetSampleSize * 1.0 / originalSize,
      seed = 0)
    else originalRDD
  val bundlePartitioner = new spark.HashPartitioner(
    2 * dataManager.runtimeContext.numAvailableCores)
  val servingRDD = sample
    .partitionBy(bundlePartitioner)
    .persist(spark.storage.StorageLevel.MEMORY_AND_DISK_2)

  private val mappedEdgeAttributes = mutable.Map[UUID, AttributeRDD[_]]()
  def mappedDataFor[T: ClassTag](attr: EdgeAttribute[T]): AttributeRDD[T] = {
    val gUID = attr.gUID
    if (!mappedEdgeAttributes.contains(gUID)) {
      assert(attr.edgeBundle == edgeBundle)
      val mapped = servingRDD.join(dataManager.get(attr).rdd)
        .mapValues { case (edge, value) => value }
        .persist(spark.storage.StorageLevel.MEMORY_AND_DISK_2)
      mappedEdgeAttributes(gUID) = mapped
    }
    mappedEdgeAttributes(gUID).asInstanceOf[AttributeRDD[T]]
  }

  val srcVertexPartitioner = dataManager.get(edgeBundle.srcVertexSet).rdd.partitioner.get
  val dstVertexPartitioner = dataManager.get(edgeBundle.srcVertexSet).rdd.partitioner.get

  val srcVertexToEdges = servingRDD
    .map { case (id, edge) => (edge.src, id) }
    .groupByKey(srcVertexPartitioner)
    .mapValues(_.toArray)
    .persist(spark.storage.StorageLevel.MEMORY_AND_DISK)
  val dstVertexToEdges = servingRDD
    .map { case (id, edge) => (edge.dst, id) }
    .groupByKey(srcVertexPartitioner)
    .mapValues(_.toArray)
    .persist(spark.storage.StorageLevel.MEMORY_AND_DISK)

  val srcAttributes = new MappedAttributes(
    edgeBundle.srcVertexSet,
    srcVertexPartitioner,
    srcVertexToEdges)
  val dstAttributes = new MappedAttributes(
    edgeBundle.dstVertexSet,
    dstVertexPartitioner,
    dstVertexToEdges)

  class MappedAttributes(vertexSet: VertexSet,
                         vertexPartitioner: spark.Partitioner,
                         vertexToEdges: rdd.RDD[(ID, Array[ID])]) {
    val mappedAttributes = mutable.Map[UUID, AttributeRDD[_]]()

    def mappedDataFor[T: ClassTag](attr: VertexAttribute[T]): AttributeRDD[T] = {
      val gUID = attr.gUID
      if (!mappedAttributes.contains(gUID)) {
        assert(attr.vertexSet == vertexSet)
        val byVertex = dataManager.get(attr).rdd
        val byEdge = byVertex.join(vertexToEdges)
          .flatMap { case (vid, (value, edges)) => edges.map((_, value)) }
          .groupByKey(bundlePartitioner)
          .mapValues(values => { assert(values.size == 1); values.head })
          .persist(spark.storage.StorageLevel.MEMORY_AND_DISK_2)
        mappedAttributes(gUID) = byEdge
      }
      mappedAttributes(gUID).asInstanceOf[AttributeRDD[T]]
    }
  }

  def attrData[T: ClassTag](markedAttr: TripletAttribute[T]): AttributeRDD[T] = {
    markedAttr match {
      case ea: EdgeAttribute[T] => mappedDataFor(ea)
      case SrcAttr(attr) => srcAttributes.mappedDataFor(attr)
      case DstAttr(attr) => dstAttributes.mappedDataFor(attr)
    }
  }
  def applyOp[P1: ClassTag, R: ClassTag](input1: TripletAttribute[P1],
                                         op: P1 => R): AttributeRDD[R] = {
    attrData(input1).mapValues(op)
  }
  def applyOp[P1: ClassTag, P2: ClassTag, R: ClassTag](input1: TripletAttribute[P1],
                                                       input2: TripletAttribute[P2],
                                                       op: (P1, P2) => R): AttributeRDD[R] = {
    attrData(input1).join(attrData(input2)).mapValues { case (i1, i2) => op(i1, i2) }
  }
  def applyOp[P1: ClassTag, P2: ClassTag, P3: ClassTag, R: ClassTag](
    input1: TripletAttribute[P1],
    input2: TripletAttribute[P2],
    input3: TripletAttribute[P3],
    op: (P1, P2, P3) => R): AttributeRDD[R] = {
    attrData(input1).join(attrData(input2)).join(attrData(input3))
      .mapValues { case ((i1, i2), i3) => op(i1, i2, i3) }
  }
  def applyOp[P1: ClassTag, P2: ClassTag, P3: ClassTag, P4: ClassTag, R: ClassTag](
    input1: TripletAttribute[P1],
    input2: TripletAttribute[P2],
    input3: TripletAttribute[P3],
    input4: TripletAttribute[P4],
    op: (P1, P2, P3, P4) => R): AttributeRDD[R] = {
    attrData(input1).join(attrData(input2)).join(attrData(input3)).join(attrData(input4))
      .mapValues { case (((i1, i2), i3), i4) => op(i1, i2, i3, i4) }
  }
}
