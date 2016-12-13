package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

object ShortestPath extends OpFromJson {
  class Input extends MagicInputSignature {
    val (vs, es) = graph
    val edgeDistance = edgeAttribute[Double](es)
    val startingDistance = vertexAttribute[Double](vs)
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

  private def calculationState(distance: SortedRDD[Long, Double]): (Long, Double) =
    (distance.count(), distance.values.sum)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val edges = inputs.es.rdd
    val edgeDistance = inputs.edgeDistance.rdd
    // distance: vertex -> (distance, iteration id when this was last updated)
    var distance = inputs.startingDistance.rdd
    // edges: source vertex -> (dest vertex, edge weight)
    val loopEdges = inputs.vs.rdd.map { case (id, _) => (id -> (id, 0.0)) }
    val edgesWithDistance =
      edges.sortedJoin(edgeDistance)
        .map { case (id, (edge, weight)) => (edge.src -> (edge.dst, weight)) }
        .union(loopEdges)
        .sort(distance.partitioner.get)

    distance.cache()
    var iterationId = 1
    var lastState = calculationState(distance)
    var unchanged = false
    while (iterationId <= maxIterations && !unchanged) {
      distance = edgesWithDistance
        .sortedJoin(distance)
        .map {
          case (src, ((dest, weight), distance)) =>
            dest -> (distance + weight)
        }
        .reduceBySortedKey(distance.partitioner.get, Math.min)
      distance.cache()

      iterationId += 1
      val currentState = calculationState(distance)
      unchanged = lastState == currentState
      lastState = currentState
    }
    output(o.distance, distance)
  }
}
