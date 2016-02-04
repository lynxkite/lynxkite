// Trains a neural network on a graph and uses it to predict an attribute.
package com.lynxanalytics.biggraph.graph_operations

import breeze.linalg._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object NeuralNetwork extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
    val label = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = NeuralNetwork()
}
import NeuralNetwork._
case class NeuralNetwork() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj()

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
    val label = inputs.label.rdd.coalesce(1)
    val label = inputs.label.rdd.coalesce(1)
    val prediction = label.mapPartitions(labels => predict(labels, edges, reversed))

    output(o.prediction, prediction.sortUnique(inputs.vertices.partitioner.get))
  }

  val hiddenSize = 100
  val iterations = 10
  def predict(
    // TODO: Pass in vertex set too.
    labelIterator: Iterator[(ID, Double)],
    edges: CompactUndirectedGraph,
    reversed: CompactUndirectedGraph): Iterator[(ID, Double)] = {
    val labels = labelIterator.toMap
    val states = // TODO: Initial hidden state.
    for (i <- 1 to iterations) {
    }
  }
}
