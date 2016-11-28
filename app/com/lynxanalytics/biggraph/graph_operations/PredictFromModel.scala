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
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val modelValue = inputs.model.value
    val rddArray = inputs.features.toArray.map(_.rdd)
    val featuresRDD = Model.toLinalgVector(rddArray, inputs.vertices.rdd)
    val featuresDF = featuresRDD.toDF("ID", "vector")
    val partitioner = featuresRDD.partitioner.get
    // Transform data to an attributeRDD with the attribute prediction
    val transformation = modelValue.load(rc.sparkContext).transformDF(featuresDF)
    val prediction = transformation.select("ID", "prediction").map { row =>
      (row.getAs[ID]("ID"), row.getAs[Double]("prediction"))
    }.rdd.sortUnique(partitioner)
    output(o.prediction, prediction)
  }
}
