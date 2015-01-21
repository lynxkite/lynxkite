package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object JoinMoreAttributes extends OpFromJson {
  class Input[T](attrCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val attrs = (0 until attrCount).map(i => vertexAttribute[T](vs, Symbol("attr-" + i)))
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.attrs(0).typeTag
    val attr = vertexAttribute[Array[T]](inputs.vs.entity)
  }
  def fromJson(j: play.api.libs.json.JsValue) = ??? // TODO: Delete this file.
}
import JoinMoreAttributes._
case class JoinMoreAttributes[T: play.api.libs.json.Reads](attrCount: Int, defaultValue: T) extends TypedMetaGraphOp[Input[T], Output[T]] {
  @transient override lazy val inputs = new Input[T](attrCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attrs(0).meta.classTag

    val noAttrs = inputs.vs.rdd.mapValues(_ => new Array[T](attrCount))
    val indexed = inputs.attrs.zipWithIndex

    val joined = indexed.foldLeft(noAttrs) {
      case (rdd, (attr, i)) => join(rdd, attr.rdd, i)
    }
    output(o.attr, joined)
  }

  def join(rdd: AttributeRDD[Array[T]], attr: AttributeRDD[T], i: Int): AttributeRDD[Array[T]] = {
    rdd.sortedLeftOuterJoin(attr)
      .mapValues {
        case (attrs, value) =>
          attrs(i) = value.getOrElse(defaultValue)
          attrs
      }
  }
}
