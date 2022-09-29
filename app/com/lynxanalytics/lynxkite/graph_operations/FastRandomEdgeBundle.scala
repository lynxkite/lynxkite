// Creates a random-generated edge bundle.
//
// The degrees will be approximately uniformly distributed between 0 and 2 * averageDegree.

package com.lynxanalytics.lynxkite.graph_operations

import scala.util.Random

import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.rdd

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

object FastRandomEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val es = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: JsValue) = FastRandomEdgeBundle(
    (j \ "seed").as[Int],
    (j \ "averageDegree").as[Double])
}
import FastRandomEdgeBundle._
case class FastRandomEdgeBundle(seed: Int, averageDegree: Double)
    extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = Json.obj("seed" -> seed, "averageDegree" -> averageDegree)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val vs = inputs.vs.rdd
    val partitioner = new HashPartitioner(
      vs.partitioner.get.numPartitions * averageDegree.ceil.toInt)
    val inCandidates = randomCopies(vs, averageDegree, seed, partitioner)
    val outCandidates = randomCopies(vs, averageDegree, seed * seed + 42 * seed + 77, partitioner)
    val randomEdges = inCandidates.zipPartitions(outCandidates, true) { (it1, it2) =>
      it1.zip(it2).map { case (id1, id2) => Edge(id1, id2) }
    }
    output(o.es, randomEdges.randomNumbered(partitioner))
  }

  private def randomCopies(
      vs: VertexSetRDD,
      averageCopies: Double,
      seed: Int,
      partitioner: Partitioner): rdd.RDD[ID] = {

    val numPartitions = partitioner.numPartitions
    vs
      .mapPartitionsWithIndex {
        case (pidx, it) =>
          val rand = new Random((pidx << 16) + seed)
          it.flatMap {
            case (id, _) =>
              val copies = math.round(rand.nextFloat() * 2 * averageCopies).toInt
              Iterator
                .continually(id)
                .take(copies)
                .map(value => rand.nextInt(numPartitions) -> value)
          }
      }
      .partitionBy(partitioner)
      .values
      .mapPartitionsWithIndex {
        case (pidx, it) =>
          val rand = new Random((pidx << 16) + seed)
          rand.shuffle(it)
      }
  }
}
