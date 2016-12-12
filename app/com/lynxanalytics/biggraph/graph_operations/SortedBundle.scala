// Builds a graph from a set of edges provided as a vertex set and two string attributes (src/dst).
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.RDDUtils

import org.apache.spark

object SortedBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val es = edgeBundle(vsA, vsB)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val sb = hybridEdgeBundle(inputs.es.entity)
  }
  def fromJson(j: JsValue) = SortedBundle()
}
import SortedBundle._
case class SortedBundle() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val edges = inputs.es.rdd
    val partitioner = edges.partitioner.get
    val bySrc = HybridRDD.of(edges.map { case (_, Edge(src, dst)) => src -> dst }, partitioner, even = true)
    output(o.sb, bySrc)
  }
}
