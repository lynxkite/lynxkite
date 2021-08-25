// Creates an edge attribute that is the pair of source and destination vertex IDs.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object EdgeBundleAsAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance,
      inputs: Input)
      extends MagicOutput(instance) {
    val attr = edgeAttribute[(ID, ID)](inputs.edges.entity)
  }
  def fromJson(j: JsValue) = EdgeBundleAsAttribute()
}
import EdgeBundleAsAttribute._
case class EdgeBundleAsAttribute() extends SparkOperation[Input, Output] {

  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.attr, inputs.edges.rdd.mapValues(edge => (edge.src, edge.dst)))
  }
}

// A variant where you get separate src/dst attributes.
object EdgeBundleAsTwoAttributes extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance,
      inputs: Input)
      extends MagicOutput(instance) {
    val src = edgeAttribute[ID](inputs.edges.entity)
    val dst = edgeAttribute[ID](inputs.edges.entity)
  }
  def fromJson(j: JsValue) = EdgeBundleAsTwoAttributes()
}
case class EdgeBundleAsTwoAttributes()
    extends SparkOperation[EdgeBundleAsTwoAttributes.Input, EdgeBundleAsTwoAttributes.Output] {
  @transient override lazy val inputs = new EdgeBundleAsTwoAttributes.Input()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new EdgeBundleAsTwoAttributes.Output()(instance, inputs)
  def execute(
      inputDatas: DataSet,
      o: EdgeBundleAsTwoAttributes.Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.src, inputs.edges.rdd.mapValues(edge => edge.src))
    output(o.dst, inputs.edges.rdd.mapValues(edge => edge.dst))
  }
}
