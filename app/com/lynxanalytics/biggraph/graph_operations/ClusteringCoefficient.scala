package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._

case class ClusteringCoefficient(outputAttribute: String) extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('es, 'vs -> 'vs, create = true)
    .outputVertexAttribute[Double]('clustering, 'vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val nonLoopEdges = inputs.edgeBundles('es).rdd.filter { case (_, e) => e.src != e.dst }
    val vertices = inputs.vertexSets('vs).rdd
    val vertexPartitioner = vertices.partitioner.get

    val inNeighbors = nonLoopEdges
      .map { case (_, e) => e.dst -> e.src }
      .groupByKey(vertexPartitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val outNeighbors = nonLoopEdges
      .map { case (_, e) => e.src -> e.dst }
      .groupByKey(vertexPartitioner)
      .mapValues(it => SortedSet(it.toSeq: _*).toArray)

    val neighbors = vertices.leftOuterJoin(outNeighbors).leftOuterJoin(inNeighbors)
      .mapValues {
        case ((_, outs), ins) => sortedUnion(outs.getOrElse(Array()), ins.getOrElse(Array()))
      }

    val outNeighborsOfNeighbors = neighbors.join(outNeighbors).flatMap {
      case (vid, (all, outs)) => all.map((_, outs))
    }.groupByKey(vertexPartitioner)

    val clusteringCoeff = neighbors.join(outNeighborsOfNeighbors).mapValues {
      case (mine, theirs) =>
        val numNeighbors = mine.size
        if (numNeighbors > 1) {
          val edgesInNeighborhood = theirs
            .map(his => sortedIntersectionSize(his, mine))
            .sum
          edgesInNeighborhood * 1.0 / numNeighbors / (numNeighbors - 1)
        } else {
          1.0
        }
    }

    outputs.putVertexAttribute('clustering, clusteringCoeff)
  }

  private def sortedUnion(a: Array[ID], b: Array[ID]): Array[ID] = {
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

  private def sortedIntersectionSize(a: Array[ID], b: Array[ID]): Int = {
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
}
