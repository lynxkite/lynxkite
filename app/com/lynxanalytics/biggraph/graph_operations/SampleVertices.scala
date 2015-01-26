package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SampleVertices extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sample = scalar[Seq[ID]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = SampleVertices((j \ "n").as[Int])
}
import SampleVertices._
case class SampleVertices(n: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("n" -> n)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas

    val vs = inputs.vs.rdd
    val sampleOrSo = vs.takeFirstNValuesOrSo(n * 2).collect.map(_._1)
    val sizeOrSo = sampleOrSo.size
    val sample = {
      if (sizeOrSo >= n) sampleOrSo.take(n)
      else vs.take(n).map(_._1)
    }.toSeq

    output(o.sample, sample)
  }
}
