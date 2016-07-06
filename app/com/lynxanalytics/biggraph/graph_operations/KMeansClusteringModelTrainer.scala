// Train a k-means clustering model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.ml.clustering.KMeans

object KMeansClusteringModelTrainer extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val model = scalar[Model]
  }
  def fromJson(j: JsValue) = KMeansClusteringModelTrainer(
    (j \ "isClassification").as[Boolean],
    (j \ "k").as[Int],
    (j \ "maxIter").as[Int],
    (j \ "seed").as[Int],
    (j \ "featureNames").as[List[String]])
}
import KMeansClusteringModelTrainer._
case class KMeansClusteringModelTrainer(
    isClassification: Boolean = true,
    k: Int,
    maxIter: Int,
    seed: Long,
    featureNames: List[String]) extends TypedMetaGraphOp[Input, Output] with ModelMeta {
  override val isHeavy = true
  @transient override lazy val inputs = new Input(featureNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "isClassification" -> isClassification,
    "k" -> k,
    "maxIter" -> maxIter,
    "seed" -> seed,
    "featureNames" -> featureNames)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val rddArray = inputs.features.map(_.rdd).toArray
    val params = new Scaler(forSGD = false).scaleFeatures(rddArray, inputs.vertices.rdd)
    val scaledDF = params.vectors.toDF("ID", "vector")

    // Train a k-means model from the scaled vectors.
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIter(maxIter)
      .setTol(0) // The convergence of the algorithm is controlled by maximum number of iterations.
      .setSeed(seed)
      .setFeaturesCol("vector")
      .setPredictionCol("prediction")
    val model = kmeans.fit(scaledDF)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    output(o.model, Model(
      isClassification = isClassification,
      method = "KMeans clustering",
      labelName = None,
      symbolicPath = file.symbolicName,
      featureNames = featureNames,
      labelScaler = None,
      featureScaler = params.featureScaler)
    )
  }
}

