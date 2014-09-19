package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.MapBucketer
import com.lynxanalytics.biggraph.spark_util.Implicits._

case class SampledViewVertex(id: Long, size: Double, label: String)

object SampledView {
  class Input(hasSizes: Boolean, hasLabels: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val ids = vertexAttribute[ID](vertices)
    val filtered = vertexSet
    val sizeAttr = if (hasSizes) vertexAttribute[Double](vertices) else null
    val labelAttr = if (hasLabels) vertexAttribute[String](vertices) else null
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val svVertices = scalar[Seq[SampledViewVertex]]
    val indexingSeq = scalar[Seq[BucketedAttribute[_]]]
  }
}
import SampledView._

case class SampledView(
    idToIdx: Map[ID, Int],
    hasSizes: Boolean,
    hasLabels: Boolean,
    maxCount: Int = 1000) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input(hasSizes, hasLabels)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    implicit val instance = output.instance

    val filtered = inputs.filtered.rdd

    val sizes = if (hasSizes) {
      inputs.sizeAttr.rdd
    } else {
      filtered.mapValues(_ => 1.0)
    }
    val labels = if (hasLabels) {
      inputs.labelAttr.rdd
    } else {
      filtered.mapValues(_ => "")
    }
    val svVerticesMap = filtered.sortedLeftOuterJoin(sizes).sortedLeftOuterJoin(labels)
      .take(maxCount)
      .map {
        case (id, (((), size), label)) =>
          (idToIdx(id), SampledViewVertex(id, size.getOrElse(0.0), label.getOrElse("")))
      }
      .toMap

    val maxKey = svVerticesMap.keys.max
    val svVertices = (0 to maxKey)
      .map(svVerticesMap.get(_).getOrElse(SampledViewVertex(-1, 0, "")))
      .toSeq

    output(o.svVertices, svVertices)
    output(o.indexingSeq, Seq(BucketedAttribute(inputs.ids, MapBucketer(idToIdx))))
  }
}
