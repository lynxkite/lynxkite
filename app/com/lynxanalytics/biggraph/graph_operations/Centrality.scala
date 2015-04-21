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
    val vertexCount = vertices.count

    val hll = new HyperLogLogMonoid( /*bit size = */ 8)
    var hyperBallCounters = vertices
      .mapValuesWithKeys { case (key, _) => hll(key) }

    hyperBallCounters = hyperBallCounters.sortedJoin(edges.map { case (id, edge) => (edge.src, edge.dst) }.groupBySortedKey(vertexPartitioner))
      .flatMap { case (_, (hll, neighbours)) => neighbours.map(id => (id, hll)) }
      .reduceBySortedKey(vertexPartitioner, _ + _)

    val result = vertices.sortedLeftOuterJoin(hyperBallCounters).mapValues(_._2.size)
    output(o.harmonicCentrality, result)
  }
}

