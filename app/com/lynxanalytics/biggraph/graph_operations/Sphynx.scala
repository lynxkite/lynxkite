// Sphynx-only operations. They only need a signature and serialization.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object Node2Vec extends OpFromJson {
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: GraphInput) extends MagicOutput(instance) {
    val embedding = vertexAttribute[Vector[Double]](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = Node2Vec(
    (j \ "dimensions").as[Int],
    (j \ "iterations").as[Int],
    (j \ "walkLength").as[Int],
    (j \ "walksPerNode").as[Int],
    (j \ "contextSize").as[Int])
}
case class Node2Vec(dimensions: Int, iterations: Int, walkLength: Int, walksPerNode: Int, contextSize: Int)
  extends TypedMetaGraphOp[GraphInput, Node2Vec.Output] {
  @transient override lazy val inputs = new GraphInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Node2Vec.Output()(instance, inputs)
  override def toJson = Json.obj(
    "dimensions" -> dimensions, "iterations" -> iterations, "walkLength" -> walkLength,
    "walksPerNode" -> walksPerNode, "contextSize" -> contextSize)
}

object TSNE extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val vector = vertexAttribute[Vector[Double]](vs)
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val embedding = vertexAttribute[(Double, Double)](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = TSNE((j \ "perplexity").as[Double])
}
case class TSNE(perplexity: Double) extends TypedMetaGraphOp[TSNE.Input, TSNE.Output] {
  @transient override lazy val inputs = new TSNE.Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new TSNE.Output()(instance, inputs)
  override def toJson = Json.obj("perplexity" -> perplexity)
}

object PyTorchGeometricDataset extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vs, es) = graph
    val x = vertexAttribute[Vector[Double]](vs)
    val y = vertexAttribute[Double](vs)
  }
  def fromJson(j: JsValue) = PyTorchGeometricDataset((j \ "name").as[String])
}
case class PyTorchGeometricDataset(name: String) extends TypedMetaGraphOp[NoInput, PyTorchGeometricDataset.Output] {
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new PyTorchGeometricDataset.Output()(instance)
  override def toJson = Json.obj("name" -> name)
}
