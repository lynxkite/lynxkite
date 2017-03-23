// Calculates the correlation of two Double attributes.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.mllib.stat.Statistics

import com.lynxanalytics.biggraph.graph_api._

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
case class CorrelateAttributes() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val attrA = inputs.attrA.rdd
    val attrB = inputs.attrB.rdd
    val joined = attrA.sortedJoin(attrB).values
    val a = joined.keys
    val distinctA = a.distinct().count()
    assert(distinctA > 1, "Correlation is not defined because the first attribute is constant.")
    val b = joined.values
    val distinctB = b.distinct().count()
    assert(distinctB > 1, "Correlation is not defined because the second attribute is constant.")
    val correlation = Statistics.corr(a, b, "pearson") // we could do "spearman" too
    output(o.correlation, correlation)
  }
}
