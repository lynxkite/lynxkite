// Estimates Harmonic Centrality for each vertex using the HyperBall algorithm.
// Note that this algorithm does not take weights into account.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

import com.twitter.algebird._
import com.twitter.algebird.HyperLogLog._

object Centrality extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val harmonicCentrality = vertexAttribute[Double](inputs.vs.entity)
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
    // Hll counters are used to estimate set sizes.
    val globalHll = new HyperLogLogMonoid( /*bit size = */ 12)

    var hyperBallCounters = vertices
      .mapValuesWithKeys {
        // Initialize a counter for every vertex 
        case (key, _) => globalHll(key)
      }

    // We have to keep track of the HyperBall sizes for the actual
    // and the previous diameter.
    var hyperBallSizes = vertices.mapValues { _ => (1, 1) }
    var harmonicCentralities = vertices.mapValues { _ => 0.0 }
    var keepGoing = true
    var diameter = 1.0

    do {
      val actualDiameter = diameter
      hyperBallCounters = getNextHyperBall(
        hyperBallCounters, vertexPartitioner, edges, globalHll)
      hyperBallSizes = hyperBallSizes.sortedJoin(hyperBallCounters).mapValues {
        case ((_, newValue), hll) =>
          (newValue, hll.estimatedSize.toInt)
      }
      harmonicCentralities = harmonicCentralities
        .sortedJoin(hyperBallSizes)
        .mapValues {
          case (original, (oldSize, newSize)) => {
            original + ((newSize - oldSize).toDouble / actualDiameter)
          }
        }

      // The algorithm should halt if no counter changes.
      keepGoing = hyperBallSizes.map {
        case (_, (oldSize, newSize)) => newSize > oldSize
      }.reduce(_ || _)
      diameter = diameter + 1.0
    } while (keepGoing)
    output(o.harmonicCentrality, harmonicCentralities)
  }

  /** Returns hyperBallCounters for a diameter increased with 1.*/
  private def getNextHyperBall(
    hyperBallCounters: SortedRDD[ID, HLL],
    vertexPartitioner: Partitioner,
    edges: EdgeBundleRDD,
    globalHll: HyperLogLogMonoid): SortedRDD[ID, HLL] = {
    // Aggregate the Hll counters for every neighbour.
    val hyperBallOfNeighbours = hyperBallCounters
      .sortedJoin(edges.map { case (id, edge) => (edge.src, edge.dst) }
        .groupBySortedKey(vertexPartitioner))
      .flatMap {
        case (_, (hll, neighbours)) => neighbours.map(id => (id, hll))
      }
      // Note that the + operator is defined on Algebird's HLL.
      .reduceBySortedKey(vertexPartitioner, _ + _)

    // Add the original Hll.
    val nextHyperBallCounters = hyperBallCounters
      .sortedLeftOuterJoin(hyperBallOfNeighbours).mapValues {
        // There is no counter for the neighbours of vertices with no out edges.
        case (originalHll, neighbourHll) =>
          originalHll + neighbourHll.getOrElse(globalHll.zero)
      }
    nextHyperBallCounters
  }
}

