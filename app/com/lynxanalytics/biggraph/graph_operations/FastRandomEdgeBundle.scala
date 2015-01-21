package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object FastRandomEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input)
      extends MagicOutput(instance) {
    val es = edgeBundle(inputs.vs.entity, inputs.vs.entity)
  }
  def fromJson(j: play.api.libs.json.JsValue) = FastRandomEdgeBundle((j \ "seed").as[Int], (j \ "averageDegree").as[Int])
}
import FastRandomEdgeBundle._
case class FastRandomEdgeBundle(seed: Int, averageDegree: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)
  override def toJson = play.api.libs.json.Json.obj("seed" -> seed, "averageDegree" -> averageDegree)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val vs = inputs.vs.rdd
    val inCandidates = randomCopies(vs, averageDegree, seed, rc)
    val outCandidates = randomCopies(vs, averageDegree, seed * seed + 42 * seed + 77, rc)
    val randomEdges = inCandidates.zipPartitions(outCandidates, true) { (it1, it2) =>
      it1.zip(it2).map { case (id1, id2) => Edge(id1, id2) }
    }
    output(o.es, randomEdges.randomNumbered(rc.defaultPartitioner))
  }

  private def randomCopies(
    vs: VertexSetRDD, averageCopies: Int, seed: Int, rc: RuntimeContext): rdd.RDD[ID] = {

    val partitioner = rc.defaultPartitioner
    val numPartitions = partitioner.numPartitions
    vs
      .mapPartitionsWithIndex {
        case (pidx, it) =>
          val rand = new Random((pidx << 16) + seed)
          it.flatMap {
            case (id, _) =>
              val copies = math.round(rand.nextFloat() * 2 * averageCopies)
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
