// Creates a prediction attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object PredictFromModel extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val model = scalar[Model]
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = PredictFromModel((j \ "numFeatures").as[Int])
}
import PredictFromModel._
case class PredictFromModel(numFeatures: Int)
    extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(numFeatures)
  override val isHeavy = true
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("numFeatures" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val model = inputs.model.value
    val RDDArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRDD = Model.toLinalgVector(RDDArray, inputs.vertices.rdd)
    val scaledRDD = unscaledRDD.mapValues(v => model.featureScaler.get.transform(v))
    val partitioner = scaledRDD.partitioner.get
    val ids = scaledRDD.keys // We will put back the keys with a zip.
    def predictions = if (model.method == "KMeans clustering") {
      model.load(rc.sparkContext).transform(scaledRDD.values)
    } else {
      model.scaleBack(model.load(rc.sparkContext).transform(scaledRDD.values))
    }
    output(
      o.prediction,
      ids.zip(predictions).filter(!_._2.isNaN).asUniqueSortedRDD(partitioner))
  }
}
