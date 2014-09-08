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

// This will hopefully go away when edge attributes die die die.
object VertexAttributeAsEdgeAttribute {
  class Input[T] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val idSet = vertexSet
    val edges = edgeBundle(src, dst, idSet = idSet)
    val vertexAttr = vertexAttribute[T](idSet)
  }
  class Output[T](
      implicit instance: MetaGraphOperationInstance,
      inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.vertexAttr.typeTag
    val edgeAttr = edgeAttribute[T](inputs.edges.entity)
  }
}
case class VertexAttributeAsEdgeAttribute[T]()
    extends TypedMetaGraphOp[VertexAttributeAsEdgeAttribute.Input[T], VertexAttributeAsEdgeAttribute.Output[T]] {
  import VertexAttributeAsEdgeAttribute._

  @transient override lazy val inputs = new Input[T]()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.edgeAttr, inputs.vertexAttr.rdd)
  }
}

object EdgeBundleAsVertexAttribute {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val idSet = vertexSet
    val edges = edgeBundle(src, dst, idSet = idSet)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[(ID, ID)](inputs.idSet.entity)
  }
}
case class EdgeBundleAsVertexAttribute()
    extends TypedMetaGraphOp[EdgeBundleAsVertexAttribute.Input, EdgeBundleAsVertexAttribute.Output] {
  import EdgeBundleAsVertexAttribute._

  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.attr, inputs.edges.rdd.mapValues(edge => (edge.src, edge.dst)))
  }
}
