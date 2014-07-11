package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

class AddConstantEdgeAttributeOutput[T: TypeTag](
    instance: MetaGraphOperationInstance,
    edgeBundle: EdgeBundle) extends MagicOutput(instance) {

  val attr = edgeAttribute[T](edgeBundle)
}
abstract class AddConstantEdgeAttribute[T]
    extends TypedMetaGraphOp[SimpleInputSignature, AddConstantEdgeAttributeOutput[T]] {

  implicit def tt: TypeTag[T]
  val value: T

  def inputSig = SimpleInputSignature(
    vertexSets = Set('ignoredSrc, 'ignoredDst),
    edgeBundles = Map('edges -> ('ignoredSrc, 'ignoredDst)))

  def result(instance: MetaGraphOperationInstance) =
    new AddConstantEdgeAttributeOutput(
      instance,
      instance.inputs.edgeBundles('edges))

  def execute(inputDatas: DataSet,
              o: AddConstantEdgeAttributeOutput[T],
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
