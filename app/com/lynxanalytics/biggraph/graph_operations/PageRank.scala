// PageRank is an estimate of the probability that starting from a random vertex
// a random walk will take us to a specific vertex.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PageRank extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val weights = edgeAttribute[Double](es)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val pagerank = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = PageRank(
    (j \ "dampingFactor").as[Double],
    (j \ "iterations").as[Int])
}
import PageRank._
case class PageRank(dampingFactor: Double,
                    iterations: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("dampingFactor" -> dampingFactor, "iterations" -> iterations)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    // We only keep positive weights. Otherwise per-node normalization may not make sense.
    val weights = inputs.weights.rdd.filter(_._2 > 0.0)
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val edgePartitioner = edges.partitioner.get

    val edgesWithWeights = edges.sortedJoin(weights).map {
      case (_, (edge, weight)) => edge.src -> (edge.dst, weight)
    }
    val sumWeights = edgesWithWeights
      .map { case (src, (_, weight)) => src -> weight }
      .reduceBySortedKey(edgePartitioner, _ + _)
    val targetsWithWeights = RDDUtils.hybridLookup(edgesWithWeights, sumWeights)
      .mapValues { case ((dst, weight), sumWeight) => (dst, weight / sumWeight) }
      .persist(spark.storage.StorageLevel.DISK_ONLY)

    var pageRank = vertices.mapValues(_ => 1.0).sortedRepartition(edgePartitioner)
    val vertexCount = vertices.count

    for (i <- 0 until iterations) {
      val incomingRank = RDDUtils.hybridLookup(targetsWithWeights, pageRank)
        .map {
          case (src, ((dst, weight), pr)) => dst -> pr * weight * dampingFactor
        }
        .reduceBySortedKey(edgePartitioner, _ + _)

      val totalIncoming = incomingRank.map(_._2).aggregate(0.0)(_ + _, _ + _)
      val distributedExtraWeight = (vertexCount - totalIncoming) / vertexCount

      pageRank = pageRank.sortedLeftOuterJoin(incomingRank)
        .mapValues {
          case (oldRank, incoming) => distributedExtraWeight + incoming.getOrElse(0.0)
        }
    }
    output(o.pagerank, pageRank.sortedRepartition(vertexPartitioner))
  }
}
