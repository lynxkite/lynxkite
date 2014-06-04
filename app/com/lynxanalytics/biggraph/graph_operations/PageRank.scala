package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._

case class PageRank(weightAttribute: String,
                    outputAttribute: String,
                    dampingFactor: Double,
                    iterations: Int)
    extends NewVertexAttributeOperation[Double] {
  @transient lazy val tt = typeTag[Double]

  override def isSourceListValid(sources: Seq[BigGraph]): Boolean =
    super.isSourceListValid(sources) && sources.head.edgeAttributes.canRead[Double](weightAttribute)

  override def computeHolistically(inputData: GraphData,
                                   runtimeContext: RuntimeContext,
                                   vertexPartitioner: spark.Partitioner): RDD[(VertexId, Double)] = {
    val readIdx = inputData.bigGraph.edgeAttributes.readIndex[Double](weightAttribute)
    val targetsWithWeights = inputData.edges
      .map(e => (e.srcId, (e.dstId, e.attr(readIdx))))
      .groupByKey(vertexPartitioner)
      .mapValues { it =>
        val dstW = it.toSeq
        def sumWeights(s: Seq[(VertexId, Double)]) = s.map({ case (dst, w) => w }).sum
        val total = sumWeights(dstW)
        // Collapse parallel edges.
        val collapsed = dstW.groupBy({ case (dst, w) => dst }).mapValues(sumWeights(_))
        collapsed.mapValues(w => w / total).toArray
      }

    var pageRank = inputData.vertices.mapValues(attr => 1.0).partitionBy(vertexPartitioner)
    val vertexCount = inputData.vertices.count

    for (i <- 0 until iterations) {
      val incomingRank = pageRank.join(targetsWithWeights)
        .flatMap {
          case (id, (pr, targets)) =>
            targets.map { case (tid, weight) => (tid, pr * weight * dampingFactor) }
        }
        .groupByKey(vertexPartitioner)
        .mapValues(_.sum)

      val totalIncoming = incomingRank.map(_._2).aggregate(0.0)(_ + _, _ + _)
      val distributedExtraWeight = (vertexCount - totalIncoming) / vertexCount

      pageRank = pageRank.leftOuterJoin(incomingRank)
        .mapValues {
          case (oldRank, incoming) => distributedExtraWeight + incoming.getOrElse(0.0)
        }
    }
    pageRank
  }
}
