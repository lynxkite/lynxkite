// Creates a segmentation where each segment represents a bucket of an attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.model._

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

// Creates a kmeans segmentation where each segment represents one cluster.
object KMeansModelTrainer extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
  }
  class Output(properties: EdgeBundleProperties)(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val model = scalar[Model]
  }
  def fromJson(j: JsValue) =
    KMeansModelTrainer((j \ "k").as[Int], (j \ "maxIter").as[Int], (j \ "tolerance").as[Double],
      (j \ "seed").as[Int], (j \ "featureNames").as[List[String]])
}

case class KMeansModelTrainer(k: Int, maxIter: Int, tolerance: Double, seed: Long, featureNames: List[String])
    extends TypedMetaGraphOp[KMeansModelTrainer.Input, KMeansModelTrainer.Output] with ModelMeta {
  import KMeansModelTrainer._
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output(EdgeBundleProperties.default)(instance, inputs)
  }
  override def toJson = Json.obj("k" -> k, "maxIter" -> maxIter, "tolerance" -> tolerance,
    "seed" -> seed, "featureNames" -> featureNames)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val rddArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRdd = Model.toLinalgVector(rddArray, inputs.vertices.rdd)
    val unscaledDf = unscaledRdd.toDF("ID", "unscaled")

    // Create a new column which represents the vector of selected attributes 
    val scaler = new StandardScaler().setInputCol("unscaled").setOutputCol("vector")
      .setWithStd(true).setWithMean(false)
    val kmeans = new KMeans().setK(k).setMaxIter(maxIter).setTol(tolerance).setSeed(seed)
      .setFeaturesCol("vector").setPredictionCol("prediction")
    val pipeline = new Pipeline().setStages(Array(scaler, kmeans))
    val model = pipeline.fit(unscaledDf)

    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      method = "KMeans",
      labelName = "default",
      symbolicPath = file.symbolicName,
      featureNames = featureNames,
      labelScaler = Some(new StandardScalerModel(Vectors.dense(0), Vectors.dense(0), true, true)),
      featureScaler = new StandardScalerModel(Vectors.dense(0), Vectors.dense(0), true, true))
    )
  }
}

