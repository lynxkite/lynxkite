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
    (j \ "learningRate").as[Double],
    (j \ "radius").as[Int],
    (j \ "hideState").as[Boolean],
    (j \ "forgetFraction").as[Double],
    (j \ "trainingRadius").as[Int],
    (j \ "maxTrainingVertices").as[Int],
    (j \ "minTrainingVertices").as[Int],
    (j \ "iterationsInTraining").as[Int],
    (j \ "subgraphsInTraining").as[Int],
    (j \ "numberOfTrainings").as[Int])
}
import NeuralNetwork._
case class NeuralNetwork(
    featureCount: Int,
    networkSize: Int,
    learningRate: Double,
    radius: Int,
    hideState: Boolean,
    forgetFraction: Double,
    trainingRadius: Int,
    maxTrainingVertices: Int,
    minTrainingVertices: Int,
    iterationsInTraining: Int,
    subgraphsInTraining: Int,
    numberOfTrainings: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(featureCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "featureCount" -> featureCount,
    "networkSize" -> networkSize,
    "learningRate" -> learningRate,
    "radius" -> radius,
    "hideState" -> hideState,
    "forgetFraction" -> forgetFraction,
    "trainingRadius" -> trainingRadius,
    "maxTrainingVertices" -> maxTrainingVertices,
    "minTrainingVertices" -> minTrainingVertices,
    "iterationsInTraining" -> iterationsInTraining,
    "subgraphsInTraining" -> subgraphsInTraining,
    "numberOfTrainings" -> numberOfTrainings)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {

    implicit val id = inputDatas
    val isolatedVertices: Map[ID, Seq[ID]] = inputs.vertices.rdd.keys.collect.map(id => id -> Seq()).toMap
    val edges: Seq[Edge] = inputs.edges.rdd.values.collect
    val edgeLists: Map[ID, Seq[ID]] = isolatedVertices ++ edges.groupBy(_.src).mapValues(_.map(_.dst))
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
    val random = new util.Random(0)
    val labelOpt = inputs.vertices.rdd.sortedLeftOuterJoin(inputs.label.rdd).mapValues(_._2)
    val data: SortedRDD[ID, (Option[Double], Array[Double])] = labelOpt.sortedJoin(features)

    val initialNetwork = {
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
    val network = (1 to numberOfTrainings).foldLeft(initialNetwork) {
      (previous, current) =>
        averageNetworks((1 to subgraphsInTraining).map { i =>
          val (trainingVertices, trainingEdgeLists, trainingData) =
            selectRandomSubgraph(data.collect.iterator, edgeLists, trainingRadius,
              maxTrainingVertices, minTrainingVertices, random.nextInt)
          train(trainingVertices, trainingEdgeLists, trainingData,
            previous, iterationsInTraining)
        })
    }

    val prediction = predict(data.collect.iterator, edgeLists, network).toSeq
    output(o.prediction,
      rc.sparkContext.parallelize(prediction).sortUnique(inputs.vertices.rdd.partitioner.get))
  }

  def averageNetworks(networks: Seq[neural.Network]) =
    networks.reduce(_ + _) / networks.size

  def selectRandomSubgraph(
    dataIterator: Iterator[(ID, (Option[Double], Array[Double]))],
    edgeLists: Map[ID, Seq[ID]],
    selectionRadius: Int,
    maxNumberOfVertices: Int,
    minNumberOfVertices: Int,
    seed: Int): (Seq[ID], Map[ID, Seq[ID]], Seq[(ID, (Option[Double], Array[Double]))]) = {
    val random = new util.Random(seed)
    val data = dataIterator.toSeq
    val vertices = data.map(_._1)
    def verticesAround(vertex: ID): Seq[ID] = (0 until selectionRadius).foldLeft(Seq(vertex)) {
      (previous, current) => previous.flatMap(id => id +: edgeLists(id)).distinct
    }
    var subsetOfVertices: Seq[ID] = Seq()
    while (subsetOfVertices.size < minNumberOfVertices) {
      val baseVertex = vertices(random.nextInt(vertices.size))
      subsetOfVertices = subsetOfVertices ++ verticesAround(baseVertex)
    }
    subsetOfVertices = subsetOfVertices.take(maxNumberOfVertices)
    val subsetOfEdges = subsetOfVertices.map {
      id => id -> edgeLists(id).filter(subsetOfVertices.contains(_))
    }.toMap
    val subsetOfData = data.filter(vertex => subsetOfVertices.contains(vertex._1))
    (subsetOfVertices, subsetOfEdges, subsetOfData)
  }

  def getTrueState(
    data: Seq[(ID, (Option[Double], Array[Double]))]): neural.GraphData = {
    val labels = data.flatMap { case (id, (labelOpt, features)) => labelOpt.map(id -> _) }.toMap
    val features = data.map { case (id, (labelOpt, features)) => id -> features }.toMap
    val vertices = data.map(_._1)
    // True state contains label and features.
    vertices.map { id =>
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
  }

  // Forgets the label.
  def blanked(state: neural.DoubleVector) = {
    val s = state.copy
    s(0) = 0.0
    s(1) = 0.0
    s
  }

  def train(
    vertices: Seq[ID],
    edges: Map[ID, Seq[ID]],
    data: Seq[(ID, (Option[Double], Array[Double]))],
    startingNetwork: neural.Network,
    iterations: Int): neural.Network = {
    assert(networkSize >= featureCount + 2, s"Network size must be at least ${featureCount + 2}.")
    var network = startingNetwork
    for (i <- 1 to iterations) {
      val trueState = getTrueState(data)
      val random = new util.Random(1)
      val keptState = trueState.map {
        case (id, state) =>
          if (random.nextDouble < forgetFraction) id -> blanked(state)
          else id -> state
      }
      val initialState = if (!hideState) keptState else keptState.map {
        // In "hideState" mode neighbors can see the labels but it is hidden from the node itself.
        case (id, state) => id -> blanked(state)
      }

      // Forward pass.
      val outputs = (1 until radius).scanLeft {
        network.forward(vertices, edges, keptState, "state" -> initialState)
      } { (previous, r) =>
        network.forward(vertices, edges, previous("new state"), "state" -> previous("new state"))
      }
      val finalOutputs = outputs.last("new state")

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
        import neural.Implicits._
        network.backward(vertices, edges, outputs, "new state" -> (next("state") + next.neighbors))
      }
      network = network.update(gradients, learningRate)
    }
    network
  }

  def predict(
    dataIterator: Iterator[(ID, (Option[Double], Array[Double]))],
    edges: Map[ID, Seq[ID]],
    network: neural.Network): Iterator[(ID, Double)] = {
    val data = dataIterator.toSeq
    val vertices = data.map(_._1)

    val trueState = getTrueState(data)
    val initialState = if (!hideState) trueState else trueState.map {
      // In "hideState" mode neighbors can see the labels but it is hidden from the node itself.
      case (id, state) => id -> blanked(state)
    }

    val outputs = (1 until radius).scanLeft {
      network.forward(vertices, edges, initialState, "state" -> initialState)
    } { (previous, r) =>
      network.forward(vertices, edges, previous("new state"), "state" -> previous("new state"))
    }
    outputs.last("new state").map {
      case (id, state) => id -> state(0)
    }.iterator
  }
}
