// Calculates for each vertex how close its neighborhood is to a clique.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

object ClusteringCoefficient extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, inputs: GraphInput) extends MagicOutput(instance) {
    val clustering = vertexAttribute[Double](inputs.vs.entity)
  }

  private[graph_operations] def sortedUnion(a: Array[ID], b: Array[ID]): Array[ID] = {
    val builder = new mutable.ArrayBuilder.ofLong
    var ai = 0
    var bi = 0
    while (ai < a.size && bi < b.size) {
      if (a(ai) == b(bi)) {
        builder += a(ai)
        ai += 1
        bi += 1
      } else if (a(ai) > b(bi)) {
        builder += b(bi)
        bi += 1
      } else {
        builder += a(ai)
        ai += 1
      }
    }
    for (i <- ai until a.size) builder += a(i)
    for (i <- bi until b.size) builder += b(i)
    builder.result()
  }

  private[graph_operations] def sortedIntersectionSize(a: Array[ID], b: Array[ID]): Int = {
    var ai = 0
    var bi = 0
    var result = 0
    while (ai < a.size && bi < b.size) {
      if (a(ai) == b(bi)) {
        result += 1
        ai += 1
        bi += 1
      } else if (a(ai) > b(bi)) {
        bi += 1
      } else {
        ai += 1
      }
    }
    result
  }

  private[graph_operations] case class Neighbors(
      nonLoopEdges: EdgeBundleRDD,
      partitioner: spark.Partitioner) {
    val in = nonLoopEdges
      .map { case (_, e) => e.dst -> e.src }
      .groupBySortedKey(partitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val out = nonLoopEdges
      .map { case (_, e) => e.src -> e.dst }
      .groupBySortedKey(partitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val allNoIsolated = out.fullOuterJoin(in)
      .mapValues {
        case (outs, ins) => sortedUnion(outs.getOrElse(Array()), ins.getOrElse(Array()))
      }
  }
  def fromJson(j: JsValue) = ClusteringCoefficient()
}
import ClusteringCoefficient._
case class ClusteringCoefficient() extends SparkOperation[GraphInput, Output] {
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
    val nonLoopEdges = inputs.es.rdd.filter { case (_, e) => e.src != e.dst }
    val edgePartitioner = inputs.es.rdd.partitioner.get
    val neighbors = Neighbors(nonLoopEdges, edgePartitioner)

    val allNoIsolated = neighbors.allNoIsolated.copyWithAncestorsCached
    val out = neighbors.out.copyWithAncestorsCached
    val outNeighborsOfNeighbors = allNoIsolated.sortedJoin(out)
      .flatMap {
        case (vid, (all, outs)) => all.map((_, outs))
      }
    val dataSize = outNeighborsOfNeighbors.map {
      case (_, arr) => arr.size.toLong
    }.reduce(_ + _)
    val massivePartitioner = rc.partitionerForNRows(dataSize)
    val outNeighborsOfNeighborsByKey = outNeighborsOfNeighbors.groupBySortedKey(massivePartitioner)

    val vertices = inputs.vs.rdd

    val clusteringCoeffNonIsolated =
      allNoIsolated.sortedRepartition(massivePartitioner)
        .sortedLeftOuterJoin(outNeighborsOfNeighborsByKey).mapValues {
          case (mine, theirs) =>
            val numNeighbors = mine.size
            if (numNeighbors > 1) {
              theirs match {
                case Some(ns) =>
                  val edgesInNeighborhood = ns.map(his => sortedIntersectionSize(his, mine)).sum
                  edgesInNeighborhood * 1.0 / numNeighbors / (numNeighbors - 1)
                case None => 0.0
              }
            } else {
              1.0
            }
        }.sortUnique(vertices.partitioner.get)
    val clusteringCoeff =
      vertices.sortedLeftOuterJoin(clusteringCoeffNonIsolated)
        .mapValues { case (_, cc) => cc.getOrElse(1.0) }
    output(o.clustering, clusteringCoeff)
  }
}
