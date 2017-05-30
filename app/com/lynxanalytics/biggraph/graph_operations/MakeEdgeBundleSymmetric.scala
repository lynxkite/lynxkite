// Keeps the smaller set of A->B and B->A edges.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object MakeEdgeBundleSymmetric extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val symmetric = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = MakeEdgeBundleSymmetric()
}
import MakeEdgeBundleSymmetric._
case class MakeEdgeBundleSymmetric() extends TypedMetaGraphOp[GraphInput, Output] {
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

    val edgesAB = es.map { case (id, e) => ((e.src, e.dst), 1) }.reduceByKey(_ + _)
    val edgesBA = edgesAB.map { case ((a, b), n) => ((b, a), n) }
    val joined = edgesAB.join(edgesBA)
    val fewerEdges = joined.map { case (k, (n1, n2)) => (k, (n1 min n2)) }
    val dummyIdPlusEdges = fewerEdges.flatMap {
      case ((a, b), n) => List.fill(n) { Edge(a, b) }
    }.randomNumbered(es.partitioner.get)

    output(o.symmetric, dummyIdPlusEdges)
  }
}
