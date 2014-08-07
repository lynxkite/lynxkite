package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._

object AddGaussianVertexAttribute {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vertices.entity)
  }
}
import AddGaussianVertexAttribute._
case class AddGaussianVertexAttribute() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val vertices = inputs.vertices.rdd
    output(o.attr, vertices.mapPartitionsWithIndex(
      {
        case (pid, it) =>
          val rnd = new Random(pid)
          it.map { case (vid, _) => vid -> rnd.nextGaussian() }
      },
      preservesPartitioning = true))
  }
}
