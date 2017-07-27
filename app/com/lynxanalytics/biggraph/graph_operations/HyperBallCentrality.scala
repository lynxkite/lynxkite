// Estimates Centrality for each vertex using the HyperBall algorithm.
// http://vigna.di.unimi.it/ftp/papers/HyperBall.pdf
// HyperBall uses HyperLogLog counters to estimate sizes of large sets, so
// the centrality values calculated here are approximations. Note that this
// algorithm does not take weights or parallel edges into account.
package com.lynxanalytics.biggraph.graph_operations

import scala.annotation.tailrec

import org.apache.spark._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.HLLUtils
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus

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
            (oldCentrality, diffSize, distance) =>
              oldCentrality + (diffSize.toDouble / distance)
          })
          .mapValues(_._2)

      case "Lin" =>
        getMeasures(
          rc = rc,
          inputs.vs.rdd,
          inputs.es.rdd,
          measureFunction = {
            (sumDistance, diffSize, distance) =>
              sumDistance + diffSize * distance
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
            (sumDistance, diffSize, distance) =>
              sumDistance + diffSize * distance
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
  // (oldCentrality: Double, hyperBallDiffSizeAtDiameter: Int, distance: Int) =>
  //   newCentrality: Int
  type MeasureFunction = (Double, Int, Int) => Double

  // Computes a centrality-like measure for each vertex, defined by measureFunction.
  // The result for each vertex is a pair of (coreachable set size, centrality measure).
  private def getMeasures(
    rc: RuntimeContext,
    vs: UniqueSortedRDD[ID, Unit],
    es: UniqueSortedRDD[ID, Edge],
    measureFunction: MeasureFunction): UniqueSortedRDD[ID, (Int, Double)] = {
    implicit val rcImplicit = rc
    val partitioner = RDDUtils.maxPartitioner(es.partitioner.get, vs.partitioner.get)

    val vertices = vs.sortedRepartition(partitioner)
    val originalEdges = es.map { case (id, edge) => (edge.src, edge.dst) }
    val loopEdges = vs.map { case (id, _) => (id, id) }
    val distinctEdges = (originalEdges ++ loopEdges)
      .distinct(partitioner.numPartitions)
    val edges = HybridRDD.of(
      distinctEdges,
      partitioner,
      even = true) // The RDD should be even after distinct.
    edges.persist(StorageLevel.DISK_ONLY)
    // Hll counters are used to estimate set sizes.
    val hyperBallCounters = vertices.mapValuesWithKeys {
      // Initialize a counter for every vertex
      case (vid, _) => HLLUtils(bits).hllFromObject(vid)
    }

    val result = getMeasures(
      distance = 1,
      hyperBallCounters = hyperBallCounters,
      measures = vertices.mapValues { _ => (1, 0.0) },
      measureFunction = measureFunction,
      partitioner = partitioner,
      edges = edges)
    result.sortedRepartition(vs.partitioner.get)
  }

  // Recursive helper function for the above getMeasures function.
  @tailrec private def getMeasures(
    distance: Int, // Current diameter being checked.
    hyperBallCounters: UniqueSortedRDD[ID, HyperLogLogPlus], // Coreachable sets of size `diameter` for each vertex.
    measures: UniqueSortedRDD[ID, (Int, Double)], // (coreachable set size, centrality) at `diameter - 1`
    measureFunction: MeasureFunction,
    partitioner: Partitioner,
    edges: HybridRDD[ID, ID]): UniqueSortedRDD[ID, (Int, Double)] = {

    val newHyperBallCounters = getNextHyperBalls(hyperBallCounters, partitioner, edges)
    val newMeasures = newHyperBallCounters
      .sortedJoin(measures)
      .mapValues {
        case (hll, (oldSize, oldCentrality)) => {
          val newSize = hll.cardinality.toInt
          val diffSize = newSize - oldSize
          val newCentrality = measureFunction(oldCentrality, diffSize, distance)
          (newSize, newCentrality)
        }
      }

    // Store the results of this iteration, discard the results of the previous one.
    // - In theory, using MEMORY_AND_DISK would be more efficient here. In practice,
    //   that significantly increases the time Spark spends in GC, and may even cause
    //   the master to kill unresponsive executors.
    // - If we don't do this, Spark will only compute each iteration of `measures` at
    //   the last stage. In the current code, that would mean re-doing the shuffle of each
    //   stage.
    newHyperBallCounters.persist(StorageLevel.DISK_ONLY)
    newMeasures.persist(StorageLevel.DISK_ONLY)
    newMeasures.foreach(identity)
    // Note, that unpersist does not delete shuffle files, see issues/#3147
    measures.unpersist(blocking = false)
    hyperBallCounters.unpersist(blocking = false)

    if (distance < maxDiameter) {
      getMeasures(
        distance + 1,
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
    hyperBallCounters: UniqueSortedRDD[ID, HyperLogLogPlus],
    partitioner: Partitioner,
    edges: HybridRDD[ID, ID]): UniqueSortedRDD[ID, HyperLogLogPlus] = {
    // For each vertex, add the HLL counter of every neighbor to the HLL counter
    // of the vertex. We don't need outer join because each vertex has a loop edge.
    edges
      .lookup(hyperBallCounters)
      .values // (src, (dst, hll)) => (dst, hll)
      .reduceBySortedKey(
        partitioner,
        {
          case (hll1, hll2) => HLLUtils(bits).union(hll1, hll2)
        })
  }

}

