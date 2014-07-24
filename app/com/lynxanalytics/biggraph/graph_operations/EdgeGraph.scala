package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EdgeGraph {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput) extends MagicOutput(instance) {
    val newVS = vertexSet
    val newES = edgeBundle(newVS, newVS)
    val link = edgeBundle(inputs.vs.entity, newVS)
  }
}
import EdgeGraph._
case class EdgeGraph() extends TypedMetaGraphOp[GraphInput, Output] {
  @transient override lazy val inputs = new GraphInput

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sc = rc.sparkContext
    val edgePartitioner = rc.defaultPartitioner
    val edges = inputs.es.rdd
    val newVS = edges.mapValues(_ => ())

    val edgesBySource = edges.map {
      case (id, edge) => (edge.src, id)
    }.groupByKey(edgePartitioner)
    val edgesByDest = edges.map {
      case (id, edge) => (edge.dst, id)
    }.groupByKey(edgePartitioner)

    val newES = edgesBySource.join(edgesByDest).flatMap {
      case (vid, (outgoings, incomings)) =>
        for {
          outgoing <- outgoings
          incoming <- incomings
        } yield Edge(incoming, outgoing)
    }
    output(o.newVS, newVS)
    output(o.newES, newES.fastNumbered(edgePartitioner))
    // Just to connect to the results.
    output(o.link, sc.emptyRDD[(ID, Edge)].partitionBy(edgePartitioner))
  }

  override val isHeavy = true
}
