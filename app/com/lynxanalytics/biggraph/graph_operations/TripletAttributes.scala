package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect._
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class TripletMapping() extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('input, 'src -> 'dst, create = true)
    .outputVertexAttribute[Array[ID]]('srcEdges, 'src)
    .outputVertexAttribute[Array[ID]]('dstEdges, 'dst)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val input = inputs.edgeBundles('input).rdd
    val src = inputs.vertexSets('src).rdd
    val dst = inputs.vertexSets('dst).rdd
    outputs.putVertexAttribute(
      'srcEdges,
      input
        .map { case (id, edge) => (edge.src, id) }
        .groupByKey(src.partitioner.get)
        .mapValues(_.toArray))
    outputs.putVertexAttribute(
      'dstEdges,
      input
        .map { case (id, edge) => (edge.dst, id) }
        .groupByKey(dst.partitioner.get)
        .mapValues(_.toArray))
  }
}

abstract class VertexToEdgeAttribute[T] extends MetaGraphOperation {
  implicit def tt: TypeTag[T]
  implicit def ct: ClassTag[T]

  def signature = newSignature
    .inputVertexAttribute[Array[ID]]('mapping, 'vertices, create = true)
    .inputVertexAttribute[T]('original, 'vertices)
    .inputEdgeBundle('target, 'unused_src -> 'unused_dst, create = true)
    .outputEdgeAttribute[T]('mapped_attribute, 'target)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val mapping = inputs.vertexAttributes('mapping).runtimeSafeCast[Array[ID]].rdd
    val original = inputs.vertexAttributes('original).runtimeSafeCast[T].rdd
    val target = inputs.edgeBundles('target).rdd

    outputs.putEdgeAttribute(
      'mapped_attribute,
      mapping.join(original)
        .flatMap { case (vid, (edges, value)) => edges.map((_, value)) }
        .groupByKey(target.partitioner.get)
        .mapValues(values => values.head))
  }
}

case class VertexToEdgeIntAttribute() extends VertexToEdgeAttribute[Int] {
  @transient lazy val tt = typeTag[Int]
  @transient lazy val ct = classTag[Int]
}
