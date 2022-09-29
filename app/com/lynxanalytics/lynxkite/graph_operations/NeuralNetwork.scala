// Trains a neural network on a graph and uses it to predict an attribute.
package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._

@deprecated("Use GCN boxes instead.", "3.2.1")
object PredictViaNNOnGraphV1 extends OpFromJson {
  class Input(featureCount: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
    val label = vertexAttribute[Double](vertices)
    val features = (0 until featureCount).map(i => vertexAttribute[Double](vertices, Symbol("feature-" + i)))
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = PredictViaNNOnGraphV1(
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
    (j \ "numberOfTrainings").as[Int],
    (j \ "knownLabelWeight").as[Double],
    (j \ "seed").as[Int],
    (j \ "gradientCheckOn").as[Boolean],
    (j \ "networkLayout").as[String],
  )
}
import PredictViaNNOnGraphV1._
@deprecated("Use GCN boxes instead.", "3.2.1")
case class PredictViaNNOnGraphV1(
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
    numberOfTrainings: Int,
    knownLabelWeight: Double,
    seed: Int,
    gradientCheckOn: Boolean,
    networkLayout: String)
    extends TypedMetaGraphOp[Input, Output] {
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
    "numberOfTrainings" -> numberOfTrainings,
    "knownLabelWeight" -> knownLabelWeight,
    "seed" -> seed,
    "gradientCheckOn" -> gradientCheckOn,
    "networkLayout" -> networkLayout,
  )
}
