package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect._
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object TripletMapping {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val srcEdges = vertexAttribute[Array[ID]](inputs.src.entity)
    val dstEdges = vertexAttribute[Array[ID]](inputs.dst.entity)
  }
}
case class TripletMapping() extends TypedMetaGraphOp[TripletMapping.Input, TripletMapping.Output] {
  import TripletMapping._
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.edges.rdd
    val src = inputs.src.rdd
    val bySrc = edges
      .map { case (id, edge) => (edge.src, id) }
      .groupByKey(src.partitioner.get).toSortedRDD
    output(
      o.srcEdges,
      src.sortedLeftOuterJoin(bySrc)
        .mapValues {
          case (_, Some(it)) => it.toArray
          case (_, None) => Array[ID]()
        })

    val dst = inputs.dst.rdd
    val byDst = edges
      .map { case (id, edge) => (edge.dst, id) }
      .groupByKey(dst.partitioner.get).toSortedRDD
    output(
      o.dstEdges,
      dst.sortedLeftOuterJoin(byDst)
        .mapValues {
          case (_, Some(it)) => it.toArray
          case (_, None) => Array[ID]()
        })
  }

  override val isHeavy = true
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
case class VertexToEdgeAttribute[T]()
    extends TypedMetaGraphOp[VertexToEdgeAttribute.Input[T], VertexToEdgeAttribute.Output[T]] {
  import VertexToEdgeAttribute._
  @transient override lazy val inputs = new Input[T]

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
      mapping.sortedJoin(original)
        .flatMap { case (vid, (edges, value)) => edges.map((_, value)) }
        .groupBySortedKey(target.partitioner.get)
        .mapValues(values => values.head))
  }

  override val isHeavy = true
}
