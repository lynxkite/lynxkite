// Estimates Harmonic Centrality for each vertex using the HyperBall algorithm.
// Note that this algorithm does not take weights into account.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.twitter.algebird._
import com.twitter.algebird.HyperLogLog._

object Centrality extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val harmonicCentrality = vertexAttribute[Int](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = Centrality()
}
import Centrality._
case class Centrality()
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj()

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val vertices = inputs.vs.rdd
    val vertexPartitioner = vertices.partitioner.get
    val globalHll = new HyperLogLogMonoid( /*bit size = */ 8)

    var hyperBallCounters = vertices
      .mapValuesWithKeys {
        // Initialize a counter for every vertex 
        case (key, _) => globalHll(key)
      }

    hyperBallCounters = hyperBallCounters
      .sortedJoin(edges.map { case (id, edge) => (edge.src, edge.dst) }
        .groupBySortedKey(vertexPartitioner))
      .flatMap { case (_, (hll, neighbours)) => neighbours.map(id => (id, hll)) }
      // Note that the + operator is defined on Algebird's HLL
      .reduceBySortedKey(vertexPartitioner, _ + _)

    val result = vertices.sortedLeftOuterJoin(hyperBallCounters).mapValuesWithKeys {
      // We loose the counters for vertices with no outgoing edges
      case (key, (_, hll)) => hll.getOrElse(globalHll(key)).estimatedSize.toInt
    }
    output(o.harmonicCentrality, result)
  }
}

