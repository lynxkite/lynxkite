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

object VertexToEdgeAttribute {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val mapping = vertexAttribute[Array[ID]](vertices)
    val original = vertexAttribute[T](vertices)
    val target = edgeBundle(ignoredSrc, ignoredDst)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    val mappedAttribute = edgeAttribute[T](inputs.target.entity)(inputs.original.typeTag)
  }
}
import VertexToEdgeAttribute._
case class VertexToEdgeAttribute[T]()
    extends TypedMetaGraphOp[Input[T], Output[T]] {
  @transient override lazy val inputs = new VertexToEdgeAttribute.Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val mapping = inputs.mapping.rdd
    val original = inputs.original.rdd
    val target = inputs.target.rdd

    implicit val ct = inputs.original.meta.classTag

    output(
      o.mappedAttribute,
      mapping.join(original)
        .flatMap { case (vid, (edges, value)) => edges.map((_, value)) }
        .groupByKey(target.partitioner.get)
        .mapValues(values => values.head))
  }
}
