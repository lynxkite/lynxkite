package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils.Implicit

object ConcatenateBundles {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val vsC = vertexSet
    val edgesAB = edgeBundle(vsA, vsB)
    val edgesBC = edgeBundle(vsB, vsC)
    val weightsAB = edgeAttribute[Double](edgesAB)
    val weightsBC = edgeAttribute[Double](edgesBC)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val edgesAC = edgeBundle(inputs.vsA.entity, inputs.vsC.entity)
    val weightsAC = edgeAttribute[Double](edgesAC)
  }
}
import ConcatenateBundles._
case class ConcatenateBundles() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edgesAB = inputs.edgesAB.rdd
    val edgesBC = inputs.edgesBC.rdd
    val weightsAB = inputs.weightsAB.rdd
    val weightsBC = inputs.weightsBC.rdd
    val weightedEdgesAB = edgesAB.join(weightsAB)
    val weightedEdgesBC = edgesBC.join(weightsBC)

    val partitioner = inputs.vsB.rdd.partitioner.getOrElse(rc.defaultPartitioner)
    val BA = weightedEdgesAB.map { case (_, (edge, weight)) => edge.dst -> (edge.src, weight) }.partitionBy(partitioner)
    val BC = weightedEdgesBC.map { case (_, (edge, weight)) => edge.src -> (edge.dst, weight) }.partitionBy(partitioner)

    val AC = BA.join(BC).map {
      case (_, ((vertexA, weightAB), (vertexC, weightBC))) => (Edge(vertexA, vertexC), weightAB * weightBC)
    }.reduceByKey(_ + _) // TODO: possibility to define arbitrary concat functions as JS

    val numberedAC = AC.fastNumbered(rc.defaultPartitioner)

    output(o.edgesAC, numberedAC.mapValues { case (edge, weight) => edge })
    output(o.weightsAC, numberedAC.mapValues { case (edge, weight) => weight })
  }
}
