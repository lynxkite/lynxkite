package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.spark_util.RDDUtils.Implicit
import com.lynxanalytics.biggraph.graph_api._

object AddReversedEdges {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val esPlus = edgeBundle(inputs.vs.entity, inputs.vs.entity)
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
    val esPlus = es.values.flatMap(e => Iterator(e, Edge(e.dst, e.src)))
    output(o.esPlus, esPlus.fastNumbered(es.partitioner.get))
  }
}
