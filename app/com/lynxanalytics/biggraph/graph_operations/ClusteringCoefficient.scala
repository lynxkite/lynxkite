package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

case class ClusteringCoefficient(outputAttribute: String)
    extends NewVertexAttributeOperation[Double] {
  @transient lazy val tt = typeTag[Double]

  override def computeHollistically(inputData: GraphData,
                                    runtimeContext: RuntimeContext,
                                    vertexPartitioner: spark.Partitioner): RDD[(VertexId, Double)] = {
    val nonLoopEdges = inputData.edges.filter(e => e.srcId != e.dstId)

    val outNeighbors = nonLoopEdges
      .map(e => (e.dstId, e.srcId))
      .groupByKey(vertexPartitioner)
      .mapValues(_.toSet)

    val inNeighbors = nonLoopEdges
      .map(e => (e.srcId, e.dstId))
      .groupByKey(vertexPartitioner)
      .mapValues(_.toSet)

    val neighbors = outNeighbors.join(inNeighbors).mapValues {
      case (outs, ins) => outs ++ ins
    }

    val outNeighborsOfNeighbors = neighbors.join(outNeighbors).flatMap {
      case (vid, (all, outs)) => all.map((_, outs))
    }.groupByKey(vertexPartitioner)

    neighbors.join(outNeighborsOfNeighbors).mapValues {
      case (all, it) =>
        val numNeighbors = all.size
        if (numNeighbors > 1) {
          it.map(ns => (ns & all).size).sum / numNeighbors / (numNeighbors - 1)
        } else {
          1.0
        }
    }

  }

  override def computeLocally(vid: VertexId, da: DenseAttributes): Double = 1.0
}
