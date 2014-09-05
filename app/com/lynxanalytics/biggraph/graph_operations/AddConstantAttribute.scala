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

  // TODO: remove these convenience methods when EdgeAttribute gets deleted  
  import com.lynxanalytics.biggraph.graph_api.Scripting._
  def edgeInt(eb: EdgeBundle, n: Int)(implicit mm: MetaGraphManager) = {
    val cop = AddConstantIntAttribute(n)
    cop(cop.vs, eb.asVertexSet).result.attr.asEdgeAttribute(eb)
  }
  def edgeDouble(eb: EdgeBundle, n: Double)(implicit mm: MetaGraphManager) = {
    val cop = AddConstantDoubleAttribute(n)
    cop(cop.vs, eb.asVertexSet).result.attr.asEdgeAttribute(eb)
  }
  def edgeString(eb: EdgeBundle, s: String)(implicit mm: MetaGraphManager) = {
    val cop = AddConstantStringAttribute(s)
    cop(cop.vs, eb.asVertexSet).result.attr.asEdgeAttribute(eb)
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

case class AddConstantDoubleAttribute(val value: Double)
    extends AddConstantAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}

case class AddConstantIntAttribute(val value: Int)
    extends AddConstantAttribute[Int] {
  @transient lazy val tt = typeTag[Int]
}

case class AddConstantStringAttribute(val value: String)
    extends AddConstantAttribute[String] {
  @transient lazy val tt = typeTag[String]
}
