// Scala backend stub for the "Find optimal prize collecting Steiner tree" operation.
// The actual implementation is done in the Sphynx server. I retain the
// simpler Dapcstp name here.

package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._

object Dapcstp extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val edge_costs = edgeAttribute[Double](es)
    val root_costs = vertexAttribute[Double](vs)
    val gain = vertexAttribute[Double](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val arcs = edgeAttribute[Double](inputs.es.entity)
    val nodes = vertexAttribute[Double](inputs.vs.entity)
    val roots = vertexAttribute[Double](inputs.vs.entity)
    val profit = scalar[Double]
  }

  def fromJson(j: JsValue) = Dapcstp()
}

import Dapcstp._

case class Dapcstp() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
}
