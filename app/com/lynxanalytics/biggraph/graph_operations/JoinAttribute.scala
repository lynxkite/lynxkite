// Creates a new pair-typed attribute from two existing attributes.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object JoinAttributes extends OpFromJson {
  class Input[A, B] extends MagicInputSignature {
    val vs = vertexSet
    val a = vertexAttribute[A](vs)
    val b = vertexAttribute[B](vs)
  }
  class Output[A, B](implicit instance: MetaGraphOperationInstance,
                     inputs: Input[A, B])
      extends MagicOutput(instance) {
    implicit val tta = inputs.a.typeTag
    implicit val ttb = inputs.b.typeTag
    val attr = vertexAttribute[(A, B)](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = JoinAttributes()
  def run[A, B](a: Attribute[A], b: Attribute[B])(
    implicit manager: MetaGraphManager): Attribute[(A, B)] = {

    import Scripting._
    val op = JoinAttributes[A, B]()
    op(op.a, a)(op.b, b).result.attr
  }
}
import JoinAttributes._
case class JoinAttributes[A, B]()
    extends TypedMetaGraphOp[Input[A, B], Output[A, B]] {
  @transient override lazy val inputs = new Input[A, B]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[A, B],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.a.rdd.sortedJoin(inputs.b.rdd))
  }
}
