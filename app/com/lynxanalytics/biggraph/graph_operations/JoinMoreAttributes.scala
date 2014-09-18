package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.controllers.DynamicValue
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object JoinMoreAttributes {
  class Input(attrCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val attrs = (0 until attrCount).map(i => vertexAttribute[DynamicValue](vs, Symbol("dynAttr-" + i)))
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[Array[DynamicValue]](inputs.vs.entity)
  }
}
import JoinMoreAttributes._
case class JoinMoreAttributes(attrCount: Int) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input(attrCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val joined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Array[DynamicValue]())
      inputs.attrs.zipWithIndex.foldLeft(noAttrs) {
        case (rdd, (attr, i)) =>
          rdd.sortedLeftOuterJoin(attr.rdd).mapValues {
            case (attrs, attr) =>
              attrs(i) = attr.get // TODO: what to do here?
              attrs
          }
      }
    }
    output(o.attr, joined)
  }
}
