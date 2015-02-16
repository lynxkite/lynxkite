package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

object MakePosition extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val attrA = vertexAttribute[Double](vertices)
    val attrB = vertexAttribute[Double](vertices)
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val position = vertexAttribute[(Double, Double)](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = MakePosition()
}
import MakePosition._
case class MakePosition() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val attrA = inputs.attrA.rdd
    val attrB = inputs.attrB.rdd
    output(o.position, attrA.sortedJoin(attrB))
  }
}
