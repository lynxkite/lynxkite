// Creates a prediction attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ClassifyVerticesByModel extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val model = scalar[Model]
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val classification = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = ClassifyVerticesByModel((j \ "numFeatures").as[Int])
}
import ClassifyVerticesByModel._
case class ClassifyVerticesByModel(numFeatures: Int)
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
    val rddArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRdd = Model.toLinalgVector(rddArray, inputs.vertices.rdd)

    val classification = model.load(rc.sparkContext).predict(unscaledRdd.values)
    val ids = unscaledRdd.keys

    output(
      o.classification, ids.zip(classification).filter(!_._2.isNaN).asUniqueSortedRDD(unscaledRdd.partitioner.get)
    )
  }
}
