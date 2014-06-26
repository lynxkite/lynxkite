package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class PageRank(weightAttribute: String,
                    outputAttribute: String,
                    dampingFactor: Double,
                    iterations: Int)
    extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vs)
    .inputEdgeBundle('es, 'vs -> 'vs)
    .inputEdgeAttribute[Double]('weights, 'es)
    .outputVertexAttribute[Double]('pagerank, 'vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val edges = inputs.edgeBundles('es).rdd
    val weights = inputs.edgeAttributes('weights).runtimeSafeCast[Double].rdd
    val vertices = inputs.vertexSets('vs).rdd
    val vertexPartitioner = vertices.partitioner.get
    val targetsWithWeights = edges.join(weights)
      .map { case (_, (edge, weight)) => edge.src -> (edge.dst, weight) }
      .groupByKey(vertexPartitioner)
      .mapValues { it =>
        val dstW = it.toSeq
        def sumWeights(s: Seq[(ID, Double)]) = s.map({ case (dst, w) => w }).sum
        val total = sumWeights(dstW)
        // Collapse parallel edges.
        val collapsed = dstW.groupBy({ case (dst, w) => dst }).mapValues(sumWeights(_))
        collapsed.mapValues(w => w / total).toArray
      }

    var pageRank = vertices.mapValues(attr => 1.0)
    val vertexCount = vertices.count

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
    outputs.putVertexAttribute('pageranks, pageRank)
  }
}
