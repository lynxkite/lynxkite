package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PageRank extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val edgeIds = vertexSet
    val es = edgeBundle(vs, vs, idSet = edgeIds)
    val weights = vertexAttribute[Double](edgeIds)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val pagerank = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: play.api.libs.json.JsValue) = PageRank((j \ "dampingFactor").as[Double], (j \ "iterations").as[Int])
}
import PageRank._
case class PageRank(dampingFactor: Double,
                    iterations: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val weights = inputs.weights.rdd
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val targetsWithWeights = edges.sortedJoin(weights)
      .map { case (_, (edge, weight)) => edge.src -> (edge.dst, weight) }
      .groupBySortedKey(vertexPartitioner)
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
      val incomingRank = pageRank.sortedJoin(targetsWithWeights)
        .flatMap {
          case (id, (pr, targets)) =>
            targets.map { case (tid, weight) => (tid, pr * weight * dampingFactor) }
        }
        .groupBySortedKey(vertexPartitioner)
        .mapValues(_.sum)

      val totalIncoming = incomingRank.map(_._2).aggregate(0.0)(_ + _, _ + _)
      val distributedExtraWeight = (vertexCount - totalIncoming) / vertexCount

      pageRank = pageRank.sortedLeftOuterJoin(incomingRank)
        .mapValues {
          case (oldRank, incoming) => distributedExtraWeight + incoming.getOrElse(0.0)
        }
    }
    output(o.pagerank, pageRank)
  }
}
