// HybridEdgeBundle creates an src -> dst HybridBundle from an EdgeBundle.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.RDDUtils

import org.apache.spark

object HybridEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
    val es = edgeBundle(vsA, vsB)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    // The output is hybrid bundle built from the src->dst mapping.
    val sb = hybridBundle(inputs.es.entity)
  }
  def fromJson(j: JsValue) = HybridEdgeBundle()

  import com.lynxanalytics.biggraph.graph_api.Scripting._
  def bySrc(connection: EdgeBundle)(implicit manager: MetaGraphManager): HybridBundle = {
    val op = HybridEdgeBundle()
    op(op.es, connection).result.sb
  }
  def byDst(connection: EdgeBundle)(implicit manager: MetaGraphManager): HybridBundle = {
    val op = HybridEdgeBundle()
    op(op.es, ReverseEdges.run(connection)).result.sb
  }
}
// Creates the src->dst HybridBundle mapping from an EdgeBundle.
import HybridEdgeBundle._
case class HybridEdgeBundle() extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val edges = inputs.es.rdd
    val partitioner = edges.partitioner.get
    val bySrc = HybridRDD.of(
      edges.map { case (_, Edge(src, dst)) => src -> dst },
      partitioner,
      even = true)
    output(o.sb, bySrc)
  }
}
