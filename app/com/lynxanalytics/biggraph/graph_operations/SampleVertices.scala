package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SampleVertices {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sample = scalar[Seq[ID]]
  }
}
import SampleVertices._
case class SampleVertices(n: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas

    val vs = inputs.vs.rdd
    val sampleOrSo = vs.takeFirstNValuesOrSo(n * 2).keys
    val sizeOrSo = sampleOrSo.count
    val sample = {
      if (sizeOrSo == n) sampleOrSo.collect
      else if (sizeOrSo > n) sampleOrSo.take(n)
      else vs.keys.take(n)
    }.toSeq

    output(o.sample, sample)
  }
}
