// Replaces each A->B edge with a B->A edge.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object ReverseEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val esAB = edgeBundle(vsA, vsB)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val esBA = edgeBundle(inputs.vsB.entity, inputs.vsA.entity, inputs.esAB.properties.reversed)
    val injection = edgeBundle(esBA.idSet, inputs.esAB.idSet, EdgeBundleProperties.identity)
  }
  def run(eb: EdgeBundle)(implicit manager: MetaGraphManager): EdgeBundle = {
    import Scripting._
    // Let's not reverse the edges two times accidentally.
    if (eb.source.operation.isInstanceOf[ReverseEdges]) {
      implicit val opInstance = eb.source
      opInstance.operation.asInstanceOf[ReverseEdges].inputs.esAB.entity
    } else {
      val op = ReverseEdges()
      op(op.esAB, eb).result.esBA
    }
  }
  def fromJson(j: JsValue) = ReverseEdges()
}
import ReverseEdges._
case class ReverseEdges() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val esBA = inputs.esAB.rdd.mapValues(e => Edge(e.dst, e.src))
    output(o.esBA, esBA)
    output(o.injection, inputs.esAB.rdd.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }
}
