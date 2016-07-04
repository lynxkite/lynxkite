// Reduce multiple double attributes to two double attributes by principal components analysis.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.model.Model
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.ml.feature.{ StandardScaler, PCA }

object ReduceDimensions extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vs = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vs, Symbol(s"feature-$i"))
    }
  }
  class Output(properties: EdgeBundleProperties)(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val attr1 = vertexAttribute[Double](inputs.vs.entity)
    val attr2 = vertexAttribute[Double](inputs.vs.entity)
  }
  def fromJson(j: JsValue) =
    ReduceDimensions((j \ "numFeatures").as[Int])
}

case class ReduceDimensions(numFeatures: Int)
    extends TypedMetaGraphOp[ReduceDimensions.Input, ReduceDimensions.Output] {
  import ReduceDimensions._
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output(EdgeBundleProperties.default)(instance, inputs)
  }
  override def toJson = Json.obj("numFeatures" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._

    val rddArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRdd = Model.toLinalgVector(rddArray, inputs.vs.rdd)
    val unscaledDf = unscaledRdd.toDF("id", "unscaled")

    // Scale the data and transform it to two dimensions by PCA algorithm
    val scaler = new StandardScaler().setInputCol("unscaled").setOutputCol("scaled")
      .setWithStd(true).setWithMean(true)
    val scalerModel = scaler.fit(unscaledDf)
    val scaledDF = scalerModel.transform(unscaledDf)
    val pca = new PCA().setInputCol("scaled").setOutputCol("pcaVector").setK(2)
    val pcaModel = pca.fit(scaledDF)
    val pcaDf = pcaModel.transform(scaledDF).select("id", "pcaVector")
    val partitioner = rddArray(0).partitioner.get
    val pcaRdd = pcaDf.map(row => (row.getAs[ID](0), row.getAs[DenseVector](1))).sortUnique(partitioner)
    val dim1Rdd = pcaRdd.mapValues(v => v.values(0))
    val dim2Rdd = pcaRdd.mapValues(v => v.values(1))
    output(o.attr1, dim1Rdd)
    output(o.attr2, dim2Rdd)
  }
}

