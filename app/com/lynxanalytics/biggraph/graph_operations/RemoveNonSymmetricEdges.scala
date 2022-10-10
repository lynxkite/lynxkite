// Discards all A->B edges if there is no B->A edge.
// We'll keep this only for backward compatibility; see MakeEdgeBundleSymmetric
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object RemoveNonSymmetricEdges extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput)
      extends MagicOutput(instance) {
    val symmetric = edgeBundle(inputs.vs.entity, inputs.vs.entity)
    val injection = edgeBundle(
      symmetric.idSet,
      inputs.es.idSet,
      EdgeBundleProperties.embedding)
  }
  def fromJson(j: JsValue) = RemoveNonSymmetricEdges()
}
import RemoveNonSymmetricEdges._
case class RemoveNonSymmetricEdges() extends SparkOperation[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vsPart = inputs.vs.rdd.partitioner.get
    val es = inputs.es.rdd
    val bySource = es.map {
      case (id, e) => e.src -> (id, e)
    }.groupBySortedKey(vsPart)
    val byDest = es.map {
      case (id, e) => e.dst -> e.src
    }.groupBySortedKey(vsPart).mapValues(_.toSet)
    val edges = bySource.sortedJoin(byDest).flatMap {
      case (vertexId, (outEdges, inEdgeSources)) =>
        outEdges.collect {
          case (id, outEdge) if inEdgeSources.contains(outEdge.dst) => id -> outEdge
        }
    }.sortUnique(es.partitioner.get)
    output(o.symmetric, edges)
    output(o.injection, edges.mapValuesWithKeys { case (id, _) => Edge(id, id) })
  }
}
