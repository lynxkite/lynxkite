package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

object AddConstantEdgeAttribute {
  class Input extends MagicInputSignature {
    val ignoredSrc = vertexSet
    val ignoredDst = vertexSet
    val edges = edgeBundle(ignoredSrc, ignoredDst)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input,
                  typeTag: TypeTag[T]) extends MagicOutput(instance) {
    val attr = edgeAttribute[T](inputs.edges.entity)
  }
}
import AddConstantEdgeAttribute._
abstract class AddConstantEdgeAttribute[T]
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
    val edges = inputDatas.edgeBundles('edges).rdd
    output(o.attr, edges.mapValues(_ => value))
  }
}

case class AddConstantDoubleEdgeAttribute(val value: Double)
    extends AddConstantEdgeAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}
