// Trains a neural network on a graph and uses it to predict an attribute.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._
import breeze.stats.distributions.RandBasis

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.neural
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

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
  def fromJson(j: JsValue) = NeuralNetwork(
    (j \ "featureCount").as[Int],
    (j \ "networkSize").as[Int],
    (j \ "iterations").as[Int],
    (j \ "learningRate").as[Double],
    (j \ "radius").as[Int],
    (j \ "hideState").as[Boolean],
    (j \ "forgetFraction").as[Double])
}
import NeuralNetwork._
case class NeuralNetwork(
    featureCount: Int,
    networkSize: Int,
    iterations: Int,
    learningRate: Double,
    radius: Int,
    hideState: Boolean,
    forgetFraction: Double) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(featureCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "featureCount" -> featureCount,
    "networkSize" -> networkSize,
    "iterations" -> iterations,
    "learningRate" -> learningRate,
    "radius" -> radius,
    "hideState" -> hideState,
    "forgetFraction" -> forgetFraction)

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
    val features = {
      val arrays = inputs.vertices.rdd.mapValues(_ => new Array[Double](featureCount))
      inputs.features.zipWithIndex.foldLeft(arrays) {
        case (arrays, (feature, idx)) =>
          arrays.sortedJoin(feature.rdd).mapValues {
            case (array, feature) =>
              array(idx) = feature
              array
          }
      }
    }
    val labelOpt = inputs.vertices.rdd.sortedLeftOuterJoin(inputs.label.rdd).mapValues(_._2)
    val data: SortedRDD[ID, (Option[Double], Array[Double])] = labelOpt.sortedJoin(features)
    val data1 = data.coalesce(1)
    val prediction = data1.mapPartitions(data => predict(data, edges, reversed))

    output(o.prediction, prediction.sortUnique(inputs.vertices.rdd.partitioner.get))
  }

  def predict(
    dataIterator: Iterator[(ID, (Option[Double], Array[Double]))],
    edges: CompactUndirectedGraph,
    reversed: CompactUndirectedGraph): Iterator[(ID, Double)] = {
    assert(networkSize >= featureCount + 2, s"Network size must be at least ${featureCount + 2}.")
    val data = dataIterator.toSeq
    val labels = data.flatMap { case (id, (labelOpt, features)) => labelOpt.map(id -> _) }.toMap
    val features = data.map { case (id, (labelOpt, features)) => id -> features }.toMap
    val vertices = data.map(_._1)
    // Initial state contains label and features.
    val trueState: neural.GraphData = vertices.map { id =>
      val state = DenseVector.zeros[Double](networkSize)
      if (labels.contains(id)) {
        state(0) = labels(id) // Label is in position 0.
        state(1) = 1.0 // Mark of a source of truth is in position 1.
      }
      val fs = features(id)
      for (i <- 0 until fs.size) {
        state(i + 2) = fs(i) // Features start from position 2.
      }
      id -> state
    }.toMap

    var network = {
      import neural.Gates._
      val vs = Neighbors()
      val eb = V("edge bias")
      val input = Sum(vs * M("edge matrix")) + eb
      val state = Input("state")
      val update = Sigmoid(input * M("update i") + state * M("update h"))
      val reset = Sigmoid(input * M("reset i") + state * M("reset h"))
      val tilde = Tanh(input * M("activation i") + state * reset * M("activation h"))
      neural.Network(
        size = networkSize,
        "new state" -> (state - update * state + update * tilde)
      )
    }
    var finalOutputs: neural.GraphData = null
    val random = new util.Random(0)
    for (i <- 1 to iterations) {
      // Forgets the label.
      def blanked(state: DenseVector[Double]) = {
        val s = state.copy
        s(0) = 0.0
        s(1) = 0.0
        s
      }
      // Initial states.
      val keptState = trueState.map {
        case (id, state) =>
          // Make final predictions in the last iteration. No forgetting now!
          if (i != iterations && random.nextDouble < forgetFraction) id -> blanked(state)
          else id -> state
      }
      val initialState = if (!hideState) keptState else keptState.map {
        // In "hideState" mode neighbors can see the labels but it is hidden from the node itself.
        case (id, state) => id -> blanked(state)
      }

      // Forward pass.
      val outputs = (1 until radius).scanLeft {
        network.forward(vertices, edges, initialState, "state" -> keptState)
      } { (previous, r) =>
        network.forward(vertices, edges, previous("new state"), "state" -> previous("new state"))
      }
      finalOutputs = outputs.last("new state")

      // Backward pass.
      val errors: Map[ID, Double] = data.map {
        case (id, (Some(label), features)) =>
          // The label is predicted in position 0.
          id -> (finalOutputs(id)(0) - label)
        case (id, (None, features)) =>
          id -> 0.0
      }.toMap
      val errorTotal = errors.values.map(e => e * e).sum
      log.info(s"Total error in iteration $i: $errorTotal")
      val finalGradient: neural.GraphData = errors.map {
        case (id, error) =>
          val vec = DenseVector.zeros[Double](networkSize)
          vec(0) = error
          id -> vec
      }
      val gradients = outputs.init.scanRight {
        network.backward(vertices, edges, outputs.last, "new state" -> finalGradient)
      } { (outputs, next) =>
        network.backward(vertices, edges, outputs, "new state" -> next)
      }
      network = network.update(gradients)
    }
    // Return last predictions.
    finalOutputs.map {
      case (id, state) => id -> state(0)
    }.iterator
  }
}
