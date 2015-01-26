package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

/*
 * Creates a 'role' vertexAttribute with its value randomly set to "test" or "train"
 * based on ratio.
 */
object CreateRole extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val role = vertexAttribute[String](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = CreateRole((j \ "ratio").as[Double], (j \ "seed").as[Int])
}
import CreateRole._
case class CreateRole(ratio: Double, seed: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("ratio" -> ratio, "seed" -> seed)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val vertices = inputs.vertices.rdd
    output(o.role, vertices.mapPartitionsWithIndex(
      {
        case (pid, it) =>
          val rnd = new Random((pid << 16) + seed)
          it.map {
            case (vid, _) =>
              vid -> { if (rnd.nextDouble() < ratio) "test" else "train" }
          }
      },
      preservesPartitioning = true).toSortedRDD)
  }
}
