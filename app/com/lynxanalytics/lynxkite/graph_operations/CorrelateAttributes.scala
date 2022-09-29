// Calculates the correlation of two Double attributes.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark.mllib.stat.Statistics

import com.lynxanalytics.lynxkite.graph_api._

object CorrelateAttributes extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val attrA = vertexAttribute[Double](vertices)
    val attrB = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val correlation = scalar[Double]
  }
  def fromJson(j: JsValue) = CorrelateAttributes()
}
import CorrelateAttributes._
case class CorrelateAttributes() extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val attrA = inputs.attrA.rdd
    val attrB = inputs.attrB.rdd
    val joined = attrA.sortedJoin(attrB).values
    val a = joined.keys
    val b = joined.values
    val correlation = Statistics.corr(a, b, "pearson") // we could do "spearman" too
    assert(!correlation.isNaN, "Correlation is undefined because one of the attributes is constant.")
    output(o.correlation, correlation)
  }
}
