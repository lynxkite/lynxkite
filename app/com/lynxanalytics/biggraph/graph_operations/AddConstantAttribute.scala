package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

object AddConstantAttribute {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input,
                  typeTag: TypeTag[T]) extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }

  def doubleOrString(isDouble: Boolean, param: String) = {
    if (isDouble) {
      val d = param.toDouble
      AddConstantDoubleAttribute(d)
    } else {
      AddConstantStringAttribute(param)
    }
  }

  import Scripting._
  def run(vs: VertexSet, value: Double): Attribute[Double] = {
    implicit val manager = vs.manager
    val op = AddConstantDoubleAttribute(value)
    op(op.vs, vs).result.attr
  }
  def run(vs: VertexSet, value: Int): Attribute[Int] = {
    implicit val manager = vs.manager
    val op = AddConstantIntAttribute(value)
    op(op.vs, vs).result.attr
  }
  def run(vs: VertexSet, value: String): Attribute[String] = {
    implicit val manager = vs.manager
    val op = AddConstantStringAttribute(value)
    op(op.vs, vs).result.attr
  }
}
import AddConstantAttribute._
abstract class AddConstantAttribute[T]
    extends TypedMetaGraphOp[Input, Output[T]] {

  implicit def tt: TypeTag[T]
  val value: T

  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs, tt)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    output(o.attr, inputs.vs.rdd.mapValues(_ => value))
  }
}

object AddConstantDoubleAttribute extends OpFromJson {
  def fromJson(j: play.api.libs.json.JsValue) = AddConstantDoubleAttribute((j \ "value").as[Double])
}
case class AddConstantDoubleAttribute(value: Double)
    extends AddConstantAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}

object AddConstantIntAttribute extends OpFromJson {
  def fromJson(j: play.api.libs.json.JsValue) = AddConstantIntAttribute((j \ "value").as[Int])
}
case class AddConstantIntAttribute(value: Int)
    extends AddConstantAttribute[Int] {
  @transient lazy val tt = typeTag[Int]
}

object AddConstantStringAttribute extends OpFromJson {
  def fromJson(j: play.api.libs.json.JsValue) = AddConstantStringAttribute((j \ "value").as[String])
}
case class AddConstantStringAttribute(value: String)
    extends AddConstantAttribute[String] {
  @transient lazy val tt = typeTag[String]
}
