package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ForceLayout3D {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val positions = vertexAttribute[(Double, Double, Double)](inputs.vs.entity)
  }
}
import ForceLayout3D._
case class ForceLayout3D() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.positions, inputs.vs.rdd.mapValuesWithKeys {
      case (k, _) => ((k % 5).toDouble * 20 / 5 - 10.0, (k % 6).toDouble * 20 / 6 - 10, (k % 7).toDouble * 20 / 7 - 10)
    })
  }
}
