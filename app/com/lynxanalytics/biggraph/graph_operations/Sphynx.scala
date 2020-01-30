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
    (j \ "iterations").as[Int])
}
case class Node2Vec(dimensions: Int, iterations: Int)
extends TypedMetaGraphOp[GraphInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new GraphInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Node2Vec.Output()(instance, inputs)
  override def toJson = Json.obj("dimensions" -> dimensions, "iterations" -> iterations)
}

object TSNE extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val vector = vertexAttribute[Vector[Double]](vs)
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val embedding = vertexAttribute[Double, Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = TSNE()
}
case class TSNE() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new TSNE.Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new TSNE.Output()(instance, inputs)
  override def toJson = Json.obj()
}
