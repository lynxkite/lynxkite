// Creates a random-generated edge bundle so that the resulting graph is scale-free.
//
// The degree distribution is hoped to follow a power law. We achieve this by a form of
// preferential attachment, that is a process where vertices with high degree are likely to
// receive even more edges. We do this by an iterative process. In each iteration we choose
// random pairs of edges from the previous iteration and we connect the src vertex of one to
// the dst vertex of the other. As the selection is on an edge level, nodes with higher degree
// are more likely to receive more edges.

package com.lynxanalytics.biggraph.graph_operations

import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ScaleFreeEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val es = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = ScaleFreeEdgeBundle(
    (j \ "iterations").as[Int],
    (j \ "seed").as[Long],
    (j \ "perIterationMultiplier").as[Double])
}
import ScaleFreeEdgeBundle._
case class ScaleFreeEdgeBundle(iterations: Int, seed: Long, perIterationMultiplier: Double)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "iterations" -> iterations,
    "seed" -> seed,
    "perIterationMultiplier" -> perIterationMultiplier)

  def sample[T: ClassTag](rdd: RDD[T], multiplier: Double, seed: Long): RDD[T] = {
    rdd.mapPartitionsWithIndex {
      case (pidx, it) =>
        val pSeed = new Random((pidx << 16) + seed).nextLong()
        val rng = new JDKRandomGenerator()
        rng.setSeed(pSeed)
        val poisson = new PoissonDistribution(rng, multiplier, 0.001, 100)
        it.flatMap(x => Iterator.continually(x).take(poisson.sample()))
    }
  }

  def shuffle[T: ClassTag](rdd: RDD[T], seed: Long, partitioner: Partitioner): RDD[T] = {
    rdd
      .mapPartitionsWithIndex {
        case (pidx, it) =>
          val pSeed = new Random((pidx << 16) + seed).nextLong
          val rnd = new Random(pSeed)
          it.map(x => rnd.nextLong() -> x)
      }
      .sort(partitioner)
      .values
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vs = inputs.vs.rdd
    var partitioner = vs.partitioner.get
    var edges = vs.map { case (id, _) => Edge(id, id) }
    val masterRnd = new Random(seed)
    for (i <- (0 until iterations)) {
      val firsts = sample(edges, perIterationMultiplier, masterRnd.nextLong())
      val seconds = sample(edges, perIterationMultiplier, masterRnd.nextLong())
      val shuffledSeconds = shuffle(seconds, masterRnd.nextLong(), partitioner)
      partitioner = new HashPartitioner(
        (partitioner.numPartitions * perIterationMultiplier).ceil.toInt)
      val numberedFirsts = firsts
        .zipWithIndex
        .map(_.swap)
        .sortUnique(partitioner)
      val numberedSeconds = shuffledSeconds
        .zipWithIndex
        .map(_.swap)
        .sortUnique(partitioner)

      edges = numberedFirsts.sortedJoin(numberedSeconds)
        .mapValues { case (edge1, edge2) => Edge(edge1.src, edge2.dst) }
        .values
    }
    output(o.es, edges.randomNumbered(partitioner))
  }
}

