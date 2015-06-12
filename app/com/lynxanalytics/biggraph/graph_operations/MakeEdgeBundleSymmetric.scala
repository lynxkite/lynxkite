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
    val vsPart = inputs.vs.rdd.partitioner.get
    val es = inputs.es.rdd

    val edgesAB = es.map { case (id, e) => ((e.src, e.dst), 1) }.reduceByKey(_ + _)
    val edgesBA = edgesAB.map { case ((a, b), n) => ((b, a), n) }
    val joined = edgesAB.join(edgesBA)
    val fewerEdges = joined.map { case ((a, b), (n1, n2)) => if (n1 < n2) ((a, b), n1) else ((a, b), n2) }
    val dummyEdgesPlusID = fewerEdges.flatMap {
      case ((a, b), n) => List.fill(n) { Edge(a, b) }
    }.zipWithUniqueId()
    val dummyIDPlusEdges = dummyEdgesPlusID.map {
      case (e, i) => (i, e)
    }.toSortedRDD(es.partitioner.get)

    output(o.symmetric, dummyIDPlusEdges)
  }
}
