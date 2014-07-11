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

class VertexToEdgeAttributeOutput[T: TypeTag](
    instance: MetaGraphOperationInstance,
    target: EdgeBundle) extends MagicOutput(instance) {
  val mappedAttribute = edgeAttribute[T](target)
}
case class VertexToEdgeAttribute[T]()
    extends TypedMetaGraphOp[SimpleInputSignature, VertexToEdgeAttributeOutput[T]] {
  def inputSig = SimpleInputSignature(
    vertexSets = Set('vertices, 'unused_src, 'unused_dst),
    vertexAttributes = Map('mapping -> 'vertices, 'original -> 'vertices),
    edgeBundles = Map('target -> ('ignoredSrc, 'ignoredDst)))

  def result(instance: MetaGraphOperationInstance) = {
    implicit val tt =
      instance.inputs.vertexAttributes('original).asInstanceOf[VertexAttribute[T]].typeTag
    new VertexToEdgeAttributeOutput(
      instance,
      instance.inputs.edgeBundles('target))
  }

  def execute(inputDatas: DataSet,
              o: VertexToEdgeAttributeOutput[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val mapping = inputDatas.vertexAttributes('mapping).runtimeSafeCast[Array[ID]].rdd
    val originalData = inputDatas.vertexAttributes('original).asInstanceOf[VertexAttributeData[T]]
    val originalMeta = originalData.vertexAttribute
    val original = originalData.rdd
    val target = inputDatas.edgeBundles('target).rdd

    implicit val ct = originalMeta.classTag
    output(
      o.mappedAttribute,
      mapping.join(original)
        .flatMap { case (vid, (edges, value)) => edges.map((_, value)) }
        .groupByKey(target.partitioner.get)
        .mapValues(values => values.head))
  }
}
