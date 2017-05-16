// For each A->B edge it adds a B<-A edge.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.graph_api._

import org.apache.spark

object AddReversedEdges extends OpFromJson {
  private val addIsNewAttrParameter = NewParameter("addIsNewAttr", false)
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(addIsNewAttr: Boolean)(implicit instance: MetaGraphOperationInstance,
                                      inputs: Input) extends MagicOutput(instance) {
    val esPlus = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val newToOriginal = edgeBundle(
      esPlus.idSet, inputs.es.idSet,
      EdgeBundleProperties.surjection)
    // For backward compatibility. This is a bit more ugly than
    // Json migration, but it avoids the recalculation migration
    // comes with.
    val isNew =
      if (addIsNewAttr) edgeAttribute[Double](esPlus)
      else null
  }
  def fromJson(j: JsValue) = AddReversedEdges(
    addIsNewAttrParameter.fromJson(j)
  )
}
import AddReversedEdges._
case class AddReversedEdges(addIsNewAttr: Boolean = false) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(addIsNewAttr)(instance, inputs)

  override def toJson = addIsNewAttrParameter.toJson(addIsNewAttr)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val es = inputs.es.rdd
    val reverseAdded: SortedRDD[ID, (Edge, Double)] =
      es.flatMapValues(e => Iterator((e, 0.0), (Edge(e.dst, e.src), 1.0)))
    // The new edge bundle should have 2x as many edges and therefore partitions as the original.
    val partitioner = new spark.HashPartitioner(es.partitions.size * 2)
    val renumbered: UniqueSortedRDD[ID, (ID, (Edge, Double))] =
      reverseAdded.randomNumbered(partitioner)
    renumbered.persist(spark.storage.StorageLevel.DISK_ONLY)
    output(o.esPlus, renumbered.mapValues { case (oldId, (e, _)) => e })
    output(
      o.newToOriginal,
      renumbered.mapValuesWithKeys {
        case (newId, (oldId, (e, _))) => Edge(newId, oldId)
      })
    if (addIsNewAttr) {
      output(o.isNew,
        renumbered.mapValues {
          case (oldId, (e, attr)) => attr
        })
    }
  }
}
