package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.MapBucketer
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SampledView extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val ids = vertexAttribute[ID](vertices)
    val filtered = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val svVertices = scalar[Seq[ID]]
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
    val vertexIndices = scalar[Map[ID, Int]]
  }
  def fromJson(j: play.api.libs.json.JsValue) = SampledView(Set(), 1)
}
import SampledView._
case class SampledView(
    idSet: Set[ID],
    maxCount: Int = 1000) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    implicit val instance = output.instance

    val filtered = inputs.filtered.rdd
    val ids = idSet.toIndexedSeq.sorted.take(maxCount)
    val idFiltered = filtered.restrictToIdSet(ids)
    val svVertices = idFiltered.keys.collect.toSeq
    val idToIdx = svVertices.zipWithIndex.toMap

    output(o.svVertices, svVertices)
    output(o.indexingSeq, Seq(BucketedAttribute(inputs.ids, MapBucketer(idToIdx))))
    output(o.vertexIndices, idToIdx)
  }
}
