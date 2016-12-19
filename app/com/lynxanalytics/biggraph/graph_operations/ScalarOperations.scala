// Simple scalar operations, like arithmetic.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object ScalarLongDifference extends OpFromJson {
  class Input extends MagicInputSignature {
    val minuend = scalar[Long]
    val subtrahend = scalar[Long]
  }
  class Output(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val difference = scalar[Long]
  }
  def fromJson(j: JsValue) = ScalarLongDifference()
  def run(minuend: Scalar[Long], subtrahend: Scalar[Long])(
    implicit m: MetaGraphManager): Scalar[Long] = {
    import Scripting._
    val op = ScalarLongDifference()
    op(op.minuend, minuend)(op.subtrahend, subtrahend).result.difference
  }
}
case class ScalarLongDifference()
    extends TypedMetaGraphOp[ScalarLongDifference.Input, ScalarLongDifference.Output] {
  import ScalarLongDifference._
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.difference, inputs.minuend.value - inputs.subtrahend.value)
  }
}
