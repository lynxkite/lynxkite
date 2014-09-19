package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._

// Dynamic values wrap various types into a combined type that we unwrap on FE side
// The combined type helps us joining arbitrary number of different typed attributes.
case class DynamicValue(
  double: Double = 0.0,
  string: String = "")
// TODO: support for Vectors

object VertexAttributeToString {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[String](inputs.vs.entity)
  }
  def run[T](attr: VertexAttribute[T])(
    implicit manager: MetaGraphManager): VertexAttribute[String] = {

    import Scripting._
    val op = VertexAttributeToString[T]()
    op(op.attr, attr).result.attr
  }
}
case class VertexAttributeToString[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToString.Output[T]] {
  import VertexAttributeToString._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.toString))
  }
}

object VertexAttributeToDouble {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[String])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }
}
case class VertexAttributeToDouble()
    extends TypedMetaGraphOp[VertexAttributeInput[String], VertexAttributeToDouble.Output] {
  import VertexAttributeToDouble._
  @transient override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.flatMapValues(str =>
      if (str.nonEmpty) Some(str.toDouble) else None))
  }
}

object VertexAttributeToDynamicValue {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[DynamicValue](inputs.vs.entity)
  }
  def run[T](attr: VertexAttribute[T])(
    implicit manager: MetaGraphManager): VertexAttribute[DynamicValue] = {

    import Scripting._
    val op = VertexAttributeToDynamicValue[T]()
    op(op.attr, attr).result.attr
  }
}
case class VertexAttributeToDynamicValue[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToDynamicValue.Output[T]] {
  import VertexAttributeToDynamicValue._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    implicit val tt = inputs.attr.data.typeTag
    val attr = inputs.attr.rdd

    val dv = {
      if (typeOf[T] =:= typeOf[Double]) attr.mapValues(x => DynamicValue(double = x.asInstanceOf[Double], string = x.toString))
      else if (typeOf[T] =:= typeOf[String]) attr.mapValues(x => DynamicValue(string = x.asInstanceOf[String]))
      else {
        throw new AssertionError(s"Attribute of unsupported type: ${typeOf[T]}")
      }
    }
    output(o.attr, dv)
  }
}

object AttributeCast {
  class Output[From, To: TypeTag](
    implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[From])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[To](inputs.vs.entity)
  }
}
abstract class AttributeCast[From, To]()
    extends TypedMetaGraphOp[VertexAttributeInput[From], AttributeCast.Output[From, To]] {
  import AttributeCast._
  @transient override lazy val inputs = new VertexAttributeInput[From]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[From, To]()(tt, instance, inputs)
  def tt: TypeTag[To]

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.asInstanceOf[To]))
  }
}

case class AttributeVectorToAny[From]() extends AttributeCast[Vector[From], Vector[Any]] {
  @transient lazy val tt = typeTag[Vector[Any]]
}
