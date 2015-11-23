// Creates a random number (of a given distribution) vertex/edge attribute.
package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

abstract class Rnd(seed: Int) {
  val rnd = new Random(seed)
  def nextRandom(): Double
}

class GaussianRandom(seed: Int) extends Rnd(seed) {
  override def nextRandom(): Double = rnd.nextGaussian()
}

class UniformRandom(seed: Int) extends Rnd(seed) {
  override def nextRandom(): Double = rnd.nextDouble()
}

object RndFactory {
  def getRnd(distribution: String, seed: Int): Rnd = {
    distribution match {
      case "Gaussian" => new GaussianRandom(seed)
      case "Uniform" => new UniformRandom(seed)
    }
  }
}

object AddRandomAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }

  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }

  def fromJson(j: JsValue) = AddRandomAttribute(
    (j \ "seed").as[Int],
    (j \ "distribution").as[String])
}

import AddRandomAttribute._
case class AddRandomAttribute(seed: Int,
                              distribution: String) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("seed" -> seed, "distribution" -> distribution)
  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val vertices = inputs.vs.rdd
    output(o.attr, vertices.mapPartitionsWithIndex(
      {
        case (pid, it) =>
          val rnd = RndFactory.getRnd(distribution, (pid << 16) + seed)
          it.map { case (vid, _) => vid -> rnd.nextRandom() }
      },
      preservesPartitioning = true).asUniqueSortedRDD)
  }
}
