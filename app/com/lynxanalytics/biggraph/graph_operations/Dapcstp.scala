// Scala backend stub for the Dapcstp operation.
// The actual implementation is done in the Sphynx server

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object Dapcstp extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val cost = edgeAttribute[Double](es)
    val apcost = vertexAttribute[Double](vs)
    val ap = vertexAttribute[Double](vs)
    val gain = vertexAttribute[Double](vs)
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val path = edgeAttribute[Double](inputs.es.entity)
  }

  def fromJson(j: JsValue) = Dapcstp()
}

import Dapcstp._
case class Dapcstp() extends SphynxOperation[Input, Output] {
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
}
