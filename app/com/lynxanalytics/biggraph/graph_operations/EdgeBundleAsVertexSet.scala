package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object EdgeBundleAsVertexSet extends OpFromJson {
  class Input extends MagicInputSignature {
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val edges = edgeBundle(ignoredSrc, ignoredDst)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val equivalentVS = vertexSet
  }
  def fromJson(j: JsValue) = EdgeBundleAsVertexSet()
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

object EdgeBundleAsVertexAttribute extends OpFromJson {
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
  def fromJson(j: JsValue) = EdgeBundleAsVertexAttribute()
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
