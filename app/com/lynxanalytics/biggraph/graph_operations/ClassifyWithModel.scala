// Creates a classification attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model.Model
import com.lynxanalytics.biggraph.model.Implicits._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.ml.linalg.DenseVector

object ClassifyWithModel extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val model = scalar[Model]
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val probability = {
      val modelMeta = inputs.model.entity.modelMeta
      if (modelMeta.generatesProbability) {
        vertexAttribute[Double](inputs.vertices.entity)
      } else { null }
    }
    val classification = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = ClassifyWithModel((j \ "numFeatures").as[Int])
}
import ClassifyWithModel._
case class ClassifyWithModel(numFeatures: Int)
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
    val inputDF = Model.toDF(sqlContext, inputs.vertices.rdd, rddArray)
    val partitioner = inputs.vertices.rdd.partitioner.get
    val classificationModel = modelValue.load(rc.sparkContext)
    // Transform data to an attributeRDD with the attribute (probability, classification)
    val transformation = classificationModel.transformDF(inputDF)
    val classification = transformation.select("ID", "classification").map { row =>
      (row.getAs[ID]("ID"), row.getAs[java.lang.Number]("classification").doubleValue)
    }.rdd.sortUnique(partitioner)
    // Output the probability corresponded to the classification labels.
    if (o.probability != null) {
      val probability = transformation.select("ID", "probability", "classification").map { row =>
        val classification = row.getAs[Double]("classification").toInt
        val probability = row.getAs[DenseVector]("probability")(classification)
        (row.getAs[ID]("ID"), probability)
        // val threshold = classificationModel.asInstanceOf[biggraph.model.LogisticRegressionModelImpl]
        //   .getThreshold
        // val probability = transformation.select("ID", "probability").map { row =>
        //   var probabilityValue = row.getAs[DenseVector]("probability")(1)
        //   if (probabilityValue < threshold) {
        //     probabilityValue = 1 - probabilityValue
        //   }
        //   (row.getAs[ID]("ID"), probabilityValue)
      }.rdd.sortUnique(partitioner)
      output(o.probability, probability)
    }
    output(o.classification, classification)
  }
}
