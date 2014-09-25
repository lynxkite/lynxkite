package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.MapBucketer
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class SampledViewVertex(id: Long, attrs: Array[DynamicValue])

object SampledView {
  class Input(hasAttr: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val ids = vertexAttribute[ID](vertices)
    val filtered = vertexSet
    val attr = if (hasAttr) vertexAttribute[Array[DynamicValue]](vertices) else null
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val svVertices = scalar[Seq[SampledViewVertex]]
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
}
import SampledView._
case class SampledView(
    hasAttr: Boolean,
    maxCount: Int = 1000) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input(hasAttr)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    implicit val instance = output.instance

    val filtered = inputs.filtered.rdd
    val joined = if (hasAttr) filtered.sortedJoin(inputs.attr.rdd) else filtered.mapValues(x => (x, Array[DynamicValue]()))

    val svVertices = joined
      .take(maxCount)
      .toSeq
      .map {
        case (id, (_, arr)) => SampledViewVertex(id, arr)
      }

    val idToIdx = svVertices.zipWithIndex.map { case (svv, idx) => (svv.id, idx) }.toMap

    output(o.svVertices, svVertices)
    output(o.indexingSeq, Seq(BucketedAttribute(inputs.ids, MapBucketer(idToIdx))))
  }
}
