// Estimates Centrality for each vertex using the HyperBall algorithm.
// http://vigna.di.unimi.it/ftp/papers/HyperBall.pdf
// HyperBall uses HyperLogLog counters to estimate sizes of large sets, so
// the centrality values calculated here are approximations. Note that this
// algorithm does not take weights or parallel edges into account.
package com.lynxanalytics.biggraph.graph_operations

import scala.annotation.tailrec

import org.apache.spark._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HLL
import com.twitter.algebird.HyperLogLog._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object HyperBallCentrality extends OpFromJson {
  private val algorithmParameter = NewParameter("algorithm", "Harmonic")
  private val bitsParameter = NewParameter("bits", 8)

  class Input extends MagicInputSignature {
    val (vs, es) = graph
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val centrality = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = HyperBallCentrality(
    (j \ "maxDiameter").as[Int],
    algorithmParameter.fromJson(j),
    bitsParameter.fromJson(j))
}
import HyperBallCentrality._
case class HyperBallCentrality(maxDiameter: Int, algorithm: String, bits: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("maxDiameter" -> maxDiameter) ++
    algorithmParameter.toJson(algorithm) ++
    bitsParameter.toJson(bits)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val centralities = algorithm match {
      case "Harmonic" =>
        getMeasures(
          rc = rc,
          vs = inputs.vs.rdd,
          es = inputs.es.rdd,
          measureFunction = {
            (oldCentrality, diffSize, diameter) =>
              oldCentrality + (diffSize.toDouble / diameter)
          })
          .mapValues(_._2)

      case "Lin" =>
        getMeasures(
          rc = rc,
          inputs.vs.rdd,
          inputs.es.rdd,
          measureFunction = {
            (oldCentrality, diffSize, diameter) =>
              oldCentrality + diffSize * diameter // the measure is sum of distances
          })
          .mapValues {
            case (size, sumDistance) =>
              if (sumDistance == 0) {
                1.0 // Compute 1.0 for vertices with empty coreachable set by definition.
              } else {
                size.toDouble * size.toDouble / sumDistance.toDouble
              }
          }

      case "Average distance" =>
        getMeasures(
          rc = rc,
          vs = inputs.vs.rdd,
          es = inputs.es.rdd,
          measureFunction = {
            (oldCentrality, diffSize, diameter) =>
              oldCentrality + diffSize * diameter // the measure is sum of distances
          })
          .mapValues {
            case (size, sumDistance) =>
              val others = size - 1 // size includes the vertex itself
              if (others == 0) 0.0
              else sumDistance.toDouble / others.toDouble
          }
    }

    output(o.centrality, centralities)
  }

  // A function to compute a new centrality measure:
  // (oldCentrality: Double, hyperBallDiffSizeAtDiameter: Int, diamter: Int) =>
  //   newCentrality: Int
  type MeasureFunction = (Double, Int, Int) => Double

  // Computes a centrality-like measure for each vertex, defined by measureFunction.
  // The result for each vertex is a pair of (coreachable set size, centrality measure).
  private def getMeasures(
    rc: RuntimeContext,
    vs: UniqueSortedRDD[ID, Unit],
    es: UniqueSortedRDD[ID, Edge],
    measureFunction: MeasureFunction): UniqueSortedRDD[ID, (Int, Double)] = {

    // Get a partitioner of suitable size:
    val numEdges = es.count // edge data size is ~ 3 x Long = 24 bytes
    val numVertices = vs.count // vertex data size is ~ 2^bits bytes
    val numEffectiveRows = Math.max(numEdges, numVertices * (1 << bits) / 24)
    val partitioner = rc.partitionerForNRows(numEffectiveRows)

    val vertices = vs.sortedRepartition(partitioner)
    val edges = es
      .map { case (id, edge) => (edge.src, edge.dst) }
      .groupBySortedKey(partitioner)
      .cache()
    // Hll counters are used to estimate set sizes.
    val globalHll = new HyperLogLogMonoid(bits)
    val hyperBallCounters = vertices.mapValuesWithKeys {
      // Initialize a counter for every vertex
      case (vid, _) => globalHll(vid)
    }

    val result = getMeasures(
      diameter = 1,
      hyperBallCounters = hyperBallCounters,
      measures = vertices.mapValues { _ => (1, 0.0) },
      measureFunction = measureFunction,
      partitioner = partitioner,
      edges = edges)
    result.sortedRepartition(vs.partitioner.get)
  }

  /* Recursive helper function for the above getMeasures function. */
  @tailrec private def getMeasures(
    diameter: Int, // Current diameter being checked.
    hyperBallCounters: UniqueSortedRDD[ID, HLL], // Coreachable sets of size `diameter` for each vertex.
    measures: UniqueSortedRDD[ID, (Int, Double)], // (coreachable set size, centrality) at `diameter - 1`
    measureFunction: MeasureFunction,
    partitioner: Partitioner,
    edges: UniqueSortedRDD[ID, Iterable[ID]]): UniqueSortedRDD[ID, (Int, Double)] = {

    val newHyperBallCounters = getNextHyperBalls(hyperBallCounters, partitioner, edges)
    val newMeasures = newHyperBallCounters
      .sortedJoin(measures)
      .mapValues {
        case (hll, (oldSize, oldCentrality)) => {
          val newSize = hll.estimatedSize.toInt
          val diffSize = newSize - oldSize
          val newCentrality = measureFunction(oldCentrality, diffSize, diameter)
          (newSize, newCentrality)
        }
      }

    if (diameter < maxDiameter) {
      getMeasures(
        diameter + 1,
        newHyperBallCounters,
        newMeasures,
        measureFunction,
        partitioner,
        edges)
    } else {
      newMeasures
    }
  }

  /** Returns hyperBallCounters for a diameter increased with 1.*/
  private def getNextHyperBalls(
    hyperBallCounters: UniqueSortedRDD[ID, HLL],
    partitioner: Partitioner,
    edges: UniqueSortedRDD[ID, Iterable[ID]]): UniqueSortedRDD[ID, HLL] = {
    // For each vertex, add the HLL counter of every neighbor to the HLL counter
    // of the vertex.
    hyperBallCounters
      .sortedLeftOuterJoin(edges)
      .flatMap {
        case (id, (hll, neighbors)) =>
          // Implementation note: it looks counter-intuitive that we shuffle the own counter of
          // the vertex to the vertex itself, instead of joining it in later. But this is in fact
          // useful, because it ensures that the counters are entirely defined by shuffle files,
          // and therefore there is no need to recompute things to read them more than once.
          Seq((id, hll)) ++ neighbors.getOrElse(Seq()).map(nid => (nid, hll))
      }
      .reduceBySortedKey(partitioner, _ + _) // operator + invokes HLL add
  }

}

