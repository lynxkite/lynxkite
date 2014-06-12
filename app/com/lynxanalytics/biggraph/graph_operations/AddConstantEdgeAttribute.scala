package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

abstract class AddConstantEdgeAttribute[T] extends MetaGraphOperation {
  def signature = newSignature
    .inputGraph('ignored, 'edges)
    .outputEdgeAttribute[T]('attr, 'edges)
  implicit def tt: TypeTag[T]

  val value: T

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val edges = inputs.edgeBundles('edges).rdd
    outputs.putEdgeAttribute('attr, edges.mapValues(_ => value))
  }
}

case class AddConstantDoubleEdgeAttribute(val value: Double)
    extends AddConstantEdgeAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}
