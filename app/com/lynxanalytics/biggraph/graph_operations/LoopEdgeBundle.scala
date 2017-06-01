// Creates an edge bundle where each vertex is connected to itself.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object LoopEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val eb = edgeBundle(
      inputs.vs.entity,
      inputs.vs.entity,
      idSet = inputs.vs.entity,
      properties = EdgeBundleProperties.identity)
  }
  def fromJson(j: JsValue) = LoopEdgeBundle()
}
import LoopEdgeBundle._
case class LoopEdgeBundle() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = false
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val iLoveScalaImplicits = inputDatas

    val vertexIdPlusUnit = inputs.vs.rdd
    val loopEdges = vertexIdPlusUnit.mapValuesWithKeys { case (id, _) => Edge(id, id) }
    output(o.eb, loopEdges)
  }
}
