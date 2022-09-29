// Adds a random number for all vertices in a vertex set.
package com.lynxanalytics.lynxkite.graph_operations

import scala.util.Random

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

@deprecated("Use AddRandomAttribute instead.", "1.7.0")
object AddGaussianVertexAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = AddGaussianVertexAttribute((j \ "seed").as[Int])
  // SI-9650
  def apply(seed: Int) = new AddGaussianVertexAttribute(seed)
}
import AddGaussianVertexAttribute._
@deprecated("Use AddRandomAttribute instead.", "1.7.0")
class AddGaussianVertexAttribute(val seed: Int)
    extends SparkOperation[Input, Output] with Serializable {
  override def equals(o: Any) =
    o.isInstanceOf[AddGaussianVertexAttribute] &&
      o.asInstanceOf[AddGaussianVertexAttribute].seed == seed
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("seed" -> seed)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val vertices = inputs.vertices.rdd
    output(
      o.attr,
      vertices.mapPartitionsWithIndex(
        {
          case (pid, it) =>
            val rnd = new Random((pid << 16) + seed)
            it.map { case (vid, _) => vid -> rnd.nextGaussian() }
        },
        preservesPartitioning = true).asUniqueSortedRDD,
    )
  }
}
