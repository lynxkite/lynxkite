package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object EdgeBundleAsVertexSet {
  class Input extends MagicInputSignature {
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val edges = edgeBundle(ignoredSrc, ignoredDst)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val equivalentVS = vertexSet
  }
}
case class EdgeBundleAsVertexSet()
    extends TypedMetaGraphOp[EdgeBundleAsVertexSet.Input, EdgeBundleAsVertexSet.Output] {
  import EdgeBundleAsVertexSet._

  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.equivalentVS, inputs.edges.rdd.mapValues(_ => ()))
  }
}

object EdgeAttributeAsVertexAttribute {
  class Input[T] extends MagicInputSignature {
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val edges = edgeBundle(ignoredSrc, ignoredDst)
    val edgeAttr = edgeAttribute[T](edges)
  }
  class Output[T](
      implicit instance: MetaGraphOperationInstance,
      inputs: Input[T]) extends MagicOutput(instance) {

    implicit val tt = inputs.edgeAttr.typeTag
    val vertexAttr = vertexAttribute[T](inputs.edges.asVertexSet)
  }
}
case class EdgeAttributeAsVertexAttribute[T]()
    extends TypedMetaGraphOp[EdgeAttributeAsVertexAttribute.Input[T], EdgeAttributeAsVertexAttribute.Output[T]] {
  import EdgeAttributeAsVertexAttribute._

  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.vertexAttr, inputs.edgeAttr.rdd)
  }
}
