// Creates a random number (of a given distribution) vertex/edge attribute.
package com.lynxanalytics.lynxkite.graph_operations

import scala.util.Random

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

abstract class RandomDistribution(seed: Int) {
  val rnd = new Random(seed)
  def nextValue(): Double
}

class NormalDistribution(seed: Int) extends RandomDistribution(seed) {
  override def nextValue(): Double = rnd.nextGaussian()
}

class UniformDistribution(seed: Int) extends RandomDistribution(seed) {
  override def nextValue(): Double = rnd.nextDouble()
}

object RandomDistribution {
  val distributions = Map[String, Int => RandomDistribution](
    "Standard Normal" -> { new NormalDistribution(_) },
    "Standard Uniform" -> { new UniformDistribution(_) })
  def getNames: List[String] = distributions.keys.toList

  def apply(distribution: String, seed: Int): RandomDistribution = {
    distributions(distribution)(seed)
  }
}

object AddRandomAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }

  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }

  def fromJson(j: JsValue) = AddRandomAttribute(
    (j \ "seed").as[Int],
    (j \ "distribution").as[String])
}

import AddRandomAttribute._
case class AddRandomAttribute(
    seed: Int,
    distribution: String)
    extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("seed" -> seed, "distribution" -> distribution)
  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val vertices = inputs.vs.rdd
    output(
      o.attr,
      vertices.mapPartitionsWithIndex(
        {
          case (pid, it) =>
            val rnd = RandomDistribution(distribution, (pid << 16) + seed)
            it.map { case (vid, _) => vid -> rnd.nextValue() }
        },
        preservesPartitioning = true).asUniqueSortedRDD,
    )
  }
}
