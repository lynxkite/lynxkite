// Takes a sample of the vertex set.
// The sample is unbiased, but multiple samples will not be independent.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object SampleVertices extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sample = scalar[Seq[ID]]
  }
  def fromJson(j: JsValue) = SampleVertices((j \ "n").as[Int])
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
