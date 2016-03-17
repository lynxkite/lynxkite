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

  case class Network(
    size: Int,
    edgeMatrix: DenseMatrix[Double],
    edgeBias: DenseVector[Double],
    resetInput: DenseMatrix[Double],
    resetHidden: DenseMatrix[Double],
    updateInput: DenseMatrix[Double],
    updateHidden: DenseMatrix[Double],
    activationInput: DenseMatrix[Double],
    activationHidden: DenseMatrix[Double])
  object Network {
    def zeros(size: Int) = new Network(
      size,
      edgeMatrix = DenseMatrix.zeros[Double](size, size),
      edgeBias = DenseVector.zeros[Double](size),
      resetInput = DenseMatrix.zeros[Double](size, size),
      resetHidden = DenseMatrix.zeros[Double](size, size),
      updateInput = DenseMatrix.zeros[Double](size, size),
      updateHidden = DenseMatrix.zeros[Double](size, size),
      activationInput = DenseMatrix.zeros[Double](size, size),
      activationHidden = DenseMatrix.zeros[Double](size, size))
  }

  class NetworkOutputs(
      network: Network,
      vertices: Iterable[ID],
      edges: CompactUndirectedGraph,
      state: Map[ID, DenseVector[Double]],
      visibleState: Map[ID, DenseVector[Double]]) {
    import breeze.numerics._
    val input = vertices.map { id =>
      val inputs = edges.getNeighbors(id).map(network.edgeMatrix * visibleState(_))
      id -> (inputs.reduce(_ + _) + network.edgeBias)
    }.toMap
    val reset = vertices.map { id =>
      id -> sigmoid(network.resetInput * input(id) + network.resetHidden * state(id))
    }.toMap
    val update = vertices.map { id =>
      id -> sigmoid(network.updateInput * input(id) + network.updateHidden * state(id))
    }.toMap
    val tildeState = vertices.map { id =>
      id -> tanh(network.activationInput * input(id) + network.activationHidden * (reset(id) :* state(id)))
    }.toMap
    val newState = vertices.map { id =>
      id -> ((update(id) * (-1.0) + 1.0) :* state(id) + update(id) :* tildeState(id))
    }.toMap
  }

  class NetworkGradients(
      network: Network,
      vertices: Iterable[ID],
      edges: CompactUndirectedGraph,
      stateGradient: Map[ID, DenseVector[Double]],
      outputs: Seq[NetworkOutputs]) {
    import breeze.numerics._
    val state = vertices.map { id =>
      id -> ((update(id) * (-1.0) + 1.0) :* state(id) + update(id) :* tildeState(id))
    }.toMap
    val tildeState = vertices.map { id =>
      id -> tanh(network.activationInput * input(id) + network.activationHidden * (reset(id) :* state(id)))
    }.toMap
    val update = vertices.map { id =>
      id -> sigmoid(network.updateInput * input(id) + network.updateHidden * state(id))
    }.toMap
    val reset = vertices.map { id =>
      id -> sigmoid(network.resetInput * input(id) + network.resetHidden * state(id))
    }.toMap
    val input = vertices.map { id =>
      val inputs = edges.getNeighbors(id).map(network.edgeMatrix * state(_))
      id -> (inputs.reduce(_ + _) + network.edgeBias)
    }.toMap
  }

  val networkSize = 100
  val iterations = 10
  val depth = 10
  def predict(
    labelOptIterator: Iterator[(ID, Option[Double])],
    edges: CompactUndirectedGraph,
    reversed: CompactUndirectedGraph): Iterator[(ID, Double)] = {
    assert(networkSize >= featureCount + 2, s"Network size must be at least ${featureCount + 2}.")
    val labelOpt = labelOptIterator.toSeq
    val labels = labelOpt.flatMap { case (id, labelOpt) => labelOpt.map(id -> _) }.toMap
    val vertices = labelOpt.map(_._1)
    // Initial state contains label. TODO: Also add features.
    val blankState = vertices.map(_ -> DenseVector.zeros[Double](networkSize))
    val trueState: Map[ID, DenseVector[Double]] = vertices.map { id =>
      val state = DenseVector.zeros[Double](networkSize)
      if (labels.contains(id)) {
        state(0) = 1.0
        state(1) = labels(id)
      }
      id -> state
    }.toMap

    var network = Network.zeros(networkSize)
    for (i <- 1 to iterations) {
      // Forward pass.
      val outputs = (0 until depth).foldLeft {
        // Neighbors can see the labels (trueState) but it is hidden from the node itself (blankState).
        new NetworkOutputs(network, vertices, edges, blankState, trueState)
      } { (outputs, iteration) =>
        new NetworkOutputs(network, vertices, edges, outputs.newState, outputs.newState)
      }

      // Backward pass.
      val finalGradient = labels.map {
        case (id, label) =>
          val error = DenseVector.zeros[Double](networkSize)
          error(1) = label - outputs.last.state(1)
          id -> error
      }
      val gradients = new NetworkGradients(network, vertices, edges, finalGradient, outputs)

    }
    Iterator()
  }
}
