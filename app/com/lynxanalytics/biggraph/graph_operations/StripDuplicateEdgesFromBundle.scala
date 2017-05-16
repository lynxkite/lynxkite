// Discards duplicate A->B edges from an edge bundle; only one such edge will
// be retained. It is undefined, which edge that will be.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object StripDuplicateEdgesFromBundle extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val unique = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = StripDuplicateEdgesFromBundle()
}
import StripDuplicateEdgesFromBundle._
case class StripDuplicateEdgesFromBundle() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val es = inputs.es.rdd

    val swapped = es.map(_.swap)
    val representativeEdges = swapped.reduceByKey((oneId, anotherId) => oneId)
    val swappedBack = representativeEdges.map(_.swap)

    val esPart = es.partitioner.get
    output(o.unique, swappedBack.sortUnique(esPart))
  }
}
