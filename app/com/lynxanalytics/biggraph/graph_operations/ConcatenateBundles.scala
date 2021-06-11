// For each (A, C) pair of vertices for which one or more A->B and B->C edges exist, creates
// one new edge A->C.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ConcatenateBundles extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val vsC = vertexSet
    val edgesAB = edgeBundle(vsA, vsB)
    val edgesBC = edgeBundle(vsB, vsC)
    val weightsAB = edgeAttribute[Double](edgesAB)
    val weightsBC = edgeAttribute[Double](edgesBC)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val isFunction =
      inputs.edgesAB.properties.isFunction && inputs.edgesBC.properties.isFunction
    val isReversedFunction =
      inputs.edgesAB.properties.isReversedFunction && inputs.edgesBC.properties.isReversedFunction
    val edgesAC = edgeBundle(
      inputs.vsA.entity,
      inputs.vsC.entity,
      EdgeBundleProperties(isFunction = isFunction, isReversedFunction = isReversedFunction))
    val weightsAC = edgeAttribute[Double](edgesAC)
  }
  def fromJson(j: JsValue) = ConcatenateBundles()
}
import ConcatenateBundles._
case class ConcatenateBundles() extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edgesAB = inputs.edgesAB.rdd
    val edgesBC = inputs.edgesBC.rdd
    val weightsAB = inputs.weightsAB.rdd
    val weightsBC = inputs.weightsBC.rdd
    val weightedEdgesAB = edgesAB.sortedJoin(weightsAB)
    val weightedEdgesBC = edgesBC.sortedJoin(weightsBC)

    val partitioner = inputs.vsB.rdd.partitioner.get
    val BA = weightedEdgesAB.map { case (_, (edge, weight)) => edge.dst -> (edge.src, weight) }.sort(partitioner)
    val BC = weightedEdgesBC.map { case (_, (edge, weight)) => edge.src -> (edge.dst, weight) }.sort(partitioner)

    // can't use sortedJoin as we need to provide cartesian product for many to many relations
    val AC = BA.join(BC).map {
      case (_, ((vertexA, weightAB), (vertexC, weightBC))) => (Edge(vertexA, vertexC), weightAB * weightBC)
    }.reduceByKey(_ + _) // TODO: possibility to define arbitrary concat functions as JS

    val partitionerAB = edgesAB.partitioner.get
    val partitionerBC = edgesBC.partitioner.get
    val partitionerAC =
      if (partitionerAB.numPartitions > partitionerBC.numPartitions) partitionerAB
      else partitionerBC
    val numberedAC = AC.randomNumbered(partitionerAC)

    output(o.edgesAC, numberedAC.mapValues { case (edge, weight) => edge })
    output(o.weightsAC, numberedAC.mapValues { case (edge, weight) => weight })
  }
}
