package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.graph_api._

object AddReversedEdges {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val esPlus = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val injection = edgeBundle(
      esPlus.asVertexSet, inputs.es.asVertexSet,
      EdgeBundleProperties.surjection)
  }
}
import AddReversedEdges._
case class AddReversedEdges() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val es = inputs.es.rdd
    val reverseAdded: SortedRDD[ID, Edge] = es.flatMapValues(e => Iterator(e, Edge(e.dst, e.src)))
    val renumbered: SortedRDD[ID, (ID, Edge)] = reverseAdded.randomNumbered(es.partitioner.get)
    output(o.esPlus, renumbered.mapValues { case (oldID, e) => e })
    output(o.injection, renumbered.mapValuesWithKeys { case (newID, (oldID, e)) => Edge(newID, oldID) })
  }
}
