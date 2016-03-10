// Trains a neural network on a graph and uses it to predict an attribute.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object NeuralNetwork extends OpFromJson {
  class Input(featureCount: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
    val label = vertexAttribute[Double](vertices)
    val features = (0 until featureCount).map(
      i => vertexAttribute[Double](vertices, Symbol("feature-" + i)))
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = NeuralNetwork((j \ "featureCount").as[Int])
}
import NeuralNetwork._
case class NeuralNetwork(featureCount: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(featureCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("featureCount" -> featureCount)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val edges = CompactUndirectedGraph(rc, inputs.edges.rdd, needsBothDirections = false)
    val reversed = {
      val rdd = inputs.edges.rdd.mapValues { case Edge(src, dst) => Edge(dst, src) }
      CompactUndirectedGraph(rc, rdd, needsBothDirections = false)
    }
    // TODO: Add features.
    val labelOpt = inputs.vertices.rdd.sortedLeftOuterJoin(inputs.label.rdd).mapValues(_._2)
    val labelOpt1 = labelOpt.coalesce(1)
    val prediction = labelOpt1.mapPartitions(labelOpt => predict(labelOpt, edges, reversed))

    output(o.prediction, prediction.sortUnique(inputs.vertices.rdd.partitioner.get))
  }

  val networkSize = 100
  val iterations = 10
  def predict(
    labelOptIterator: Iterator[(ID, Option[Double])],
    edges: CompactUndirectedGraph,
    reversed: CompactUndirectedGraph): Iterator[(ID, Double)] = {
    import breeze.linalg._
    import breeze.numerics._
    assert(networkSize >= featureCount + 2, s"Network size must be at least ${featureCount + 2}.")
    val labelOpt = labelOptIterator.toSeq
    val labels = labelOpt.flatMap { case (id, labelOpt) => labelOpt.map(id -> _) }.toMap
    val vertices = labelOpt.map(_._1)
    val one = DenseVector.ones[Double](networkSize)
    // Initial state contains label. TODO: Also add features.
    var state: Map[ID, DenseVector[Double]] = vertices.map { id =>
      val state = DenseVector.zeros[Double](networkSize)
      if (labels.contains(id)) {
        state(0) = 1.0
        state(1) = labels(id)
      }
      id -> state
    }.toMap
    // The learned parameters.
    var edgeMatrix = DenseMatrix.zeros[Double](networkSize, networkSize)
    var edgeBias = DenseVector.zeros[Double](networkSize)
    var resetInput = DenseMatrix.zeros[Double](networkSize, networkSize)
    var resetHidden = DenseMatrix.zeros[Double](networkSize, networkSize)
    var updateInput = DenseMatrix.zeros[Double](networkSize, networkSize)
    var updateHidden = DenseMatrix.zeros[Double](networkSize, networkSize)
    var activationInput = DenseMatrix.zeros[Double](networkSize, networkSize)
    var activationHidden = DenseMatrix.zeros[Double](networkSize, networkSize)
    for (i <- 1 to iterations) {
      val input = vertices.map { id =>
        val inputs = edges.getNeighbors(id).map(edgeMatrix * state(_))
        id -> (inputs.reduce(_ + _) + edgeBias)
      }.toMap
      val reset = vertices.map { id =>
        id -> sigmoid(resetInput * input(id) + resetHidden * state(id))
      }.toMap
      val update = vertices.map { id =>
        id -> sigmoid(updateInput * input(id) + updateHidden * state(id))
      }.toMap
      val newState = vertices.map { id =>
        id -> tanh(activationInput * input(id) + activationHidden * (reset(id) :* state(id)))
      }.toMap
      state = vertices.map { id =>
        id -> ((one - update(id)) :* state(id) + update(id) :* newState(id))
      }.toMap
    }
    Iterator()
  }
}
