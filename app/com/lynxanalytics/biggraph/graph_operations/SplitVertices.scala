// Split vertices (sort of opposite of merge vertices)

package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object SplitVertices extends OpFromJson {
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[Long]) extends MagicOutput(instance) {

    val newVertices = vertexSet
    val belongsTo = edgeBundle(
      newVertices,
      inputs.vs.entity,
      EdgeBundleProperties(isFunction = true, isEverywhereDefined = true))
    val indexAttr = vertexAttribute[Long](newVertices)
  }
  def fromJson(j: JsValue) = SplitVertices()
}
import SplitVertices._
case class SplitVertices() extends TypedMetaGraphOp[VertexAttributeInput[Long], Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[Long]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output()(instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val repetitionAttr = inputs.attr.rdd

    val requestedNumberOfVerticesWithIndex =
      repetitionAttr.flatMapValues { numRepetitions => (0L until numRepetitions) }

    val partitioner = rc.partitionerForNRows(repetitionAttr.values.reduce(_ + _)) // Attr is Long.

    val newIdAndOldIdAndZeroBasedIndex =
      requestedNumberOfVerticesWithIndex
        .randomNumbered(partitioner)
        .persist(spark.storage.StorageLevel.DISK_ONLY)

    output(o.newVertices,
      newIdAndOldIdAndZeroBasedIndex
        .mapValues(_ => ()))
    output(o.belongsTo,
      newIdAndOldIdAndZeroBasedIndex
        .mapValuesWithKeys { case (newId, (oldId, idx)) => Edge(newId, oldId) })
    output(o.indexAttr,
      newIdAndOldIdAndZeroBasedIndex
        .mapValues { case (_, idx) => idx })
  }
}
