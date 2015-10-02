package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import org.apache.spark.Accumulator

object ShortestPath extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val weights = edgeAttribute[Double](es)
    val seed = vertexAttribute[Double](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val distance = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = ShortestPath((j \ "maxIterations").as[Int])
}
import ShortestPath._
case class ShortestPath(maxIterations: Double)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("maxIterations" -> maxIterations)

  def minWithStamp(d1: (Double, Int), d2: (Double, Int)): (Double, Int) = {
    val (distance1, stamp1) = d1
    val (distance2, stamp2) = d2
    if (distance1 < distance2 || distance1 == distance2 && stamp1 < stamp2) {
      d1
    } else {
      d2
    }
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val weights = inputs.weights.rdd
    // distance: vertex -> (distance, iteration id when this was last updated)
    var distance = inputs.seed.rdd.mapValues { distance => (distance, 0) }
    // edges: source vertex -> (dest vertex, edge weight,
    //                          hop: 0 = artifical loop edge/1 = real edge)
    val loopEdges = inputs.vs.rdd.map { case v => (v._1 -> (v._1, 0.0, 0)) }
    val edgesWithWeights =
      edges.sortedJoin(weights).
        map { case (id, (edge, weight)) => (edge.src -> (edge.dst, weight, 1)) }.
        union(loopEdges).
        toSortedRDD(distance.partitioner.get)

    var iterationId = 1
    var numChanges = 1l
    while (iterationId <= maxIterations && numChanges > 0) {
      distance = edgesWithWeights.
        sortedJoin(distance).
        map {
          case (src, ((dest, weight, hops), (distance, prevIterationId))) =>
            dest -> (distance + weight, prevIterationId + hops)
        }.
        reduceBySortedKey(distance.partitioner.get, minWithStamp)
      distance.cache()
      numChanges = distance.
        filter { case (_, (_, stamp)) => stamp == iterationId }.
        count()
      iterationId += 1
    }
    output(o.distance, distance.mapValues { case (distance, _) => distance })
  }
}
