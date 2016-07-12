// Creates a classification attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.mllib.linalg.DenseVector

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
    val probability = vertexAttribute[Double](inputs.vertices.entity)
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

    val model = inputs.model.value
    val rddArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRDD = Model.toLinalgVector(rddArray, inputs.vertices.rdd)
    val scaledRDD = unscaledRDD.mapValues(v => model.featureScaler.transform(v))
    val scaledDF = scaledRDD.toDF("ID", "vector")
    val partitioner = scaledRDD.partitioner.get
    // Transform data to an attributeRDD with the attribute (probability, prediction)
    val transformation = model.load(rc.sparkContext).transformDF(scaledDF)
      .select("ID", "probability", "prediction").map { row =>
        (row.getAs[ID]("ID"),
          (row.getAs[DenseVector]("probability"), row.getAs[java.lang.Number]("prediction"))
        )
      }
      .sortUnique(partitioner)
    // Output the probabiliy of the most likely outcome and the classification labels.
    output(o.probability, transformation.mapValues(_._1.toArray.max))
    output(o.classification, transformation.mapValues(_._2.doubleValue))
  }
}
