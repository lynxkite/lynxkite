package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import scala.collection.SortedSet
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

@SerialVersionUID(4148735639783910942l) case class ClusteringCoefficient(outputAttribute: String)
    extends NewVertexAttributeOperation[Double] {
  @transient lazy val tt = typeTag[Double]

  override def computeHolistically(inputData: GraphData,
                                   runtimeContext: RuntimeContext,
                                   vertexPartitioner: spark.Partitioner): RDD[(VertexId, Double)] = {
    val nonLoopEdges = inputData.edges.filter(e => e.srcId != e.dstId)

    val inNeighbors = nonLoopEdges
      .map(e => (e.dstId, e.srcId))
      .groupByKey(vertexPartitioner)
      .mapValues(SortedSet(_: _*).toArray)

    val outNeighbors = nonLoopEdges
      .map(e => (e.srcId, e.dstId))
      .groupByKey(vertexPartitioner)
      .mapValues(SortedSet(_: _*).toArray)

    val neighbors = inputData.vertices.leftOuterJoin(outNeighbors).leftOuterJoin(inNeighbors)
      .mapValues {
        case ((id, outs), ins) => sortedUnion(outs.getOrElse(Array()), ins.getOrElse(Array()))
      }

    val outNeighborsOfNeighbors = neighbors.join(outNeighbors).flatMap {
      case (vid, (all, outs)) => all.map((_, outs))
    }.groupByKey(vertexPartitioner)

    neighbors.join(outNeighborsOfNeighbors).mapValues {
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

  }

  override def computeLocally(vid: VertexId, da: DenseAttributes): Double = 1.0

  private def sortedUnion(a: Array[VertexId], b: Array[VertexId]): Array[VertexId] = {
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
  private def sortedIntersectionSize(a: Array[VertexId], b: Array[VertexId]): Int = {
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
