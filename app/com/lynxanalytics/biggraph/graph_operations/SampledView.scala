package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.controllers.DynamicValue
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.MapBucketer
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class SampledViewVertex(id: Long, attrs: Array[DynamicValue])

object SampledView {
  class Input() extends MagicInputSignature {
    val vertices = vertexSet
    val ids = vertexAttribute[ID](vertices)
    val filtered = vertexSet
    val attr = vertexAttribute[Array[DynamicValue]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val svVertices = scalar[Seq[SampledViewVertex]]
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
}
import SampledView._
case class SampledView(
    idToIdx: Map[ID, Int],
    maxCount: Int = 1000) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    implicit val instance = output.instance

    val filtered = inputs.filtered.rdd

    val svVerticesMap = filtered
      .sortedJoin(inputs.attr.rdd)
      .take(maxCount)
      .map {
        case (id, (_, arr)) =>
          (idToIdx(id), SampledViewVertex(id, arr))
      }
      .toMap

    val maxKey = svVerticesMap.keys.max
    val svVertices = (0 to maxKey)
      .map(svVerticesMap.get(_).getOrElse(SampledViewVertex(-1, Array())))
      .toSeq

    output(o.svVertices, svVertices)
    output(o.indexingSeq, Seq(BucketedAttribute(inputs.ids, MapBucketer(idToIdx))))
  }
}
