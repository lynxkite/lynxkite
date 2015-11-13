// For each A->B edge it adds a B<-A edge.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.graph_api._

object AddReversedEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val esPlus = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val newToOriginal = edgeBundle(
      esPlus.idSet, inputs.es.idSet,
      EdgeBundleProperties.surjection)
    val attr = edgeAttribute[Long](esPlus)
  }
  def fromJson(j: JsValue) = AddReversedEdges()
}
import AddReversedEdges._
case class AddReversedEdges() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val es = inputs.es.rdd
    val reverseAdded: SortedRDD[ID, (Edge, Long)] =
      es.flatMapValues(e => Iterator((e, 0L), (Edge(e.dst, e.src), 1L)))
    val renumbered: SortedRDD[ID, (ID, (Edge, Long))] =
      reverseAdded.randomNumbered(es.partitioner.get)
    output(o.esPlus, renumbered.mapValues { case (oldID, (e, _)) => e })
    output(
      o.newToOriginal,
      renumbered.mapValuesWithKeys {
        case (newID, (oldID, (e, _))) => Edge(newID, oldID)
      })
    output(o.attr,
      renumbered.mapValues {
        case (oldId, (e, attr)) => attr
      }.asSortedRDD(es.partitioner.get))
  }
}
