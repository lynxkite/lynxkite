// Creates a segmentation where each segment represents a bucket of an attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

// Creates a kmeans segmentation where each segment represents one cluster.
object KMeansBucketing extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vs = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vs, Symbol(s"feature-$i"))
    }
  }
  class Output(properties: EdgeBundleProperties)(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    /*val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vs.entity, segments, properties)
    val label = vertexAttribute[Int](segments)*/
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) =
    KMeansBucketing((j \ "k").as[Int], (j \ "maxIter").as[Int], (j \ "tolerance").as[Double],
      (j \ "seed").as[Int], (j \ "featureNames").as[Int])
}

case class KMeansBucketing(k: Int, maxIter: Int, tolerance: Double, seed: Long, numFeatures: Int)
    extends TypedMetaGraphOp[KMeansBucketing.Input, KMeansBucketing.Output] {
  import KMeansBucketing._
  override val isHeavy = true
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output(EdgeBundleProperties.default)(instance, inputs)
  }
  override def toJson = Json.obj("k" -> k, "maxIter" -> maxIter, "tolerance" -> tolerance,
    "seed" -> seed, "featureNames" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._
    val featuresDF = inputs.features.toArray.map(i => i.rdd.toDF("ID", i.hashCode.toString)) //the approach may not be necessary
    val joinDF = featuresDF.reduce(_ join (_, "ID"))
    // Create a new column which represents the vector of selected attributes 
    val attributesCodes = joinDF.columns.slice(1, joinDF.columns.length)
    val assembler = new VectorAssembler().setInputCols(attributesCodes).setOutputCol("vector")
    val featuresWithVector = assembler.transform(joinDF)
    val kmeans = new KMeans().setK(k).setMaxIter(maxIter).setTol(tolerance).setSeed(seed)
      .setFeaturesCol("vector").setPredictionCol("prediction")
    val model = kmeans.fit(featuresWithVector)
    // Predict the cluster centers of each data point  
    val prediction = model.transform(featuresWithVector)
    val partitioner = inputs.features(0).rdd.partitioner.get
    val predDF = prediction.select("ID", "prediction")
    val predRdd = predDF.map(row => (row.getAs[ID](0), row.getInt(1).toDouble)).sortUnique(partitioner)

    /*val bucketing = Bucketing(predRdd)
    output(o.segments, bucketing.segments)
    output(o.label, bucketing.label)
    output(o.belongsTo, bucketing.belongsTo)*/
    output(o.attr, predRdd)
  }
}

