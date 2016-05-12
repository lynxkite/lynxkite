// Creates the edge graph of a graph, where each vertex corresponds to an edge in the original graph,
// and the vertices are connected when one edge is the continuation of the other.
// See http://en.wikipedia.org/wiki/Edge_graph for a more precise definition.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EdgeGraph extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput) extends MagicOutput(instance) {
    val newVS = vertexSet
    val newES = edgeBundle(newVS, newVS)
    val link = edgeBundle(inputs.vs.entity, newVS)
  }
  def fromJson(j: JsValue) = EdgeGraph()
}
import EdgeGraph._
case class EdgeGraph() extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sc = rc.sparkContext
    val edges = inputs.es.rdd
    val edgePartitioner = edges.partitioner.get
    val newVS = edges.mapValues(_ => ())

    val edgesBySource = edges.map {
      case (id, edge) => (edge.src, id)
    }.groupBySortedKey(edgePartitioner)
    val edgesByDest = edges.map {
      case (id, edge) => (edge.dst, id)
    }.groupBySortedKey(edgePartitioner)

    val newES = edgesBySource.sortedJoin(edgesByDest).flatMap {
      case (vid, (outgoings, incomings)) =>
        for {
          outgoing <- outgoings
          incoming <- incomings
        } yield Edge(incoming, outgoing)
    }
    output(o.newVS, newVS)
    output(o.newES, newES.randomNumbered(edgePartitioner))
    // Just to connect to the results.
    output(o.link, sc.emptyRDD[(ID, Edge)].sortUnique(edgePartitioner))
  }
}
