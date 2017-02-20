// Split edges (sort of opposite of merge edges)

package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util._

object SplitEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val attr = edgeAttribute[Long](es)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {

    val newEdges = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val belongsTo = edgeBundle(
      newEdges.idSet,
      inputs.es.entity.idSet,
      EdgeBundleProperties(isFunction = true, isEverywhereDefined = true))
    val indexAttr = edgeAttribute[Long](newEdges)
  }
  def fromJson(j: JsValue) = SplitEdges()
}
import SplitEdges._
case class SplitEdges() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val edges = inputs.es.rdd
    val repetitionAttr = inputs.attr.rdd

    val newEdgesWithIndex =
      edges.sortedJoin(repetitionAttr).flatMapValues {
        case (edge, numRepetitions) => (0L until numRepetitions).map(index => (edge, index))
      }

    val partitioner = rc.partitionerForNRows(repetitionAttr.values.reduce(_ + _)) // Attr is Long.

    val newIdAndOldIdAndEdgeAndZeroBasedIndex =
      newEdgesWithIndex
        .randomNumbered(partitioner)
        .persist(spark.storage.StorageLevel.DISK_ONLY)

    output(o.newEdges,
      newIdAndOldIdAndEdgeAndZeroBasedIndex
        .mapValues { case (_, (edge, _)) => edge })
    output(o.belongsTo,
      newIdAndOldIdAndEdgeAndZeroBasedIndex
        .mapValuesWithKeys { case (newId, (oldId, (_, idx))) => Edge(newId, oldId) })
    output(o.indexAttr,
      newIdAndOldIdAndEdgeAndZeroBasedIndex
        .mapValues { case (_, (_, idx)) => idx })
  }
}
