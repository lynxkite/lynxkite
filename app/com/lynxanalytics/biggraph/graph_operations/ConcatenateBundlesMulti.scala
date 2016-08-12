// For n A->B edges and m B->C edges, creates nm new edge A->C.
// Also creates two functions:  new_AC->old_AB and newAC -> old_BC

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ConcatenateBundlesMulti extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val vsC = vertexSet
    val edgesAB = edgeBundle(vsA, vsB)
    val edgesBC = edgeBundle(vsB, vsC)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val isFunction =
      inputs.edgesAB.properties.isFunction && inputs.edgesBC.properties.isFunction
    val isReversedFunction =
      inputs.edgesAB.properties.isReversedFunction && inputs.edgesBC.properties.isReversedFunction
    val edgesAC = edgeBundle(
      inputs.vsA.entity,
      inputs.vsC.entity,
      EdgeBundleProperties(isFunction = isFunction, isReversedFunction = isReversedFunction))
    val projectionFirst = edgeBundle(edgesAC.idSet, inputs.edgesAB.idSet, EdgeBundleProperties.partialFunction)
    val projectionSecond = edgeBundle(edgesAC.idSet, inputs.edgesBC.idSet, EdgeBundleProperties.partialFunction)

  }
  def fromJson(j: JsValue) = ConcatenateBundlesMulti()
}
import ConcatenateBundlesMulti._
case class ConcatenateBundlesMulti() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edgesAB = inputs.edgesAB.rdd
    val edgesBC = inputs.edgesBC.rdd

    val partitioner = inputs.vsB.rdd.partitioner.get
    val BA = edgesAB.map { case (id, edge) => edge.dst -> (id, edge.src) }.sort(partitioner)
    val BC = edgesBC.map { case (id, edge) => edge.src -> (id, edge.dst) }.sort(partitioner)

    val AC = BA.join(BC).map {
      case (_, ((idAB, vertexA), (idBC, vertexC))) => (Edge(vertexA, vertexC), (idAB, idBC))
    }

    val partitionerAB = edgesAB.partitioner.get
    val partitionerBC = edgesBC.partitioner.get
    val partitionerAC =
      if (partitionerAB.numPartitions > partitionerBC.numPartitions) partitionerAB
      else partitionerBC
    val numberedAC = AC.randomNumbered(partitionerAC) //(idAC,(Edge,(idAB,idBC)))

    val resAC = numberedAC.mapValues { case (edge, (_, _)) => edge }
    val projectionFirst = numberedAC.mapValuesWithKeys { case (idAC, (edge, (idAB, idBC))) => Edge(idAC, idAB) }
    val projectionSecond = numberedAC.mapValuesWithKeys { case (idAC, (edge, (idAB, idBC))) => Edge(idAC, idBC) }

    output(o.edgesAC, resAC)
    output(o.projectionFirst, projectionFirst)
    output(o.projectionSecond, projectionSecond)
  }
}
