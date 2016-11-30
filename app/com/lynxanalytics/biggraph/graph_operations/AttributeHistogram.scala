// Calculates the histogram values based on a Bucketer.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object AttributeHistogram extends OpFromJson {
  private val sampleSizeParameter = NewParameter("sampleSize", 50000)

  class Input[T] extends MagicInputSignature {
    val original = vertexSet
    val filtered = vertexSet
    val attr = vertexAttribute[T](original)
    val originalCount = scalar[Long]
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val counts = scalar[Map[Int, Long]]
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type =
    AttributeHistogram(
      TypedJson.read[Bucketer[_]](j \ "bucketer"),
      sampleSizeParameter.fromJson(j))
}
import AttributeHistogram._
/**
 * @param sampleSize specifies the number of data points to use for the histogram.
 * A negative value turns sampling off and all the data points will be used.
 */
case class AttributeHistogram[T](bucketer: Bucketer[T], sampleSize: Int)
    extends TypedMetaGraphOp[Input[T], Output] {
  @transient override lazy val inputs = new Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("bucketer" -> bucketer.toTypedJson) ++
    sampleSizeParameter.toJson(sampleSize)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val attrMeta = inputs.attr.meta
    implicit val ct = attrMeta.classTag
    val filteredAttr = inputs.attr.rdd.sortedJoin(inputs.filtered.rdd)
      .mapValues { case (value, _) => value }
    val bucketedAttr = filteredAttr.flatMapValues(bucketer.whichBucket(_))
    output(
      o.counts,
      RDDUtils.estimateOrPreciseValueCounts(
        inputs.original.rdd,
        bucketedAttr,
        inputs.originalCount.value,
        sampleSize,
        rc).counts.toMap)
  }
}
