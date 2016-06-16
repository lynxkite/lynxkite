// Reduce multiple double attributes to two double attributes by principle components analysis.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

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
    ReduceDimensions((j \ "featureNames").as[Int])
}

case class ReduceDimensions(numFeatures: Int)
    extends TypedMetaGraphOp[ReduceDimensions.Input, ReduceDimensions.Output] {
  import ReduceDimensions._
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    new Output(EdgeBundleProperties.default)(instance, inputs)
  }
  override def toJson = Json.obj("featureNames" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val sqlContext = rc.dataManager.newSQLContext()
    import sqlContext.implicits._
    val featuresRDD: Array[AttributeRDD[Double]] = inputs.features.toArray.map { i => i.rdd }
    val featuresDF = featuresRDD.map { i => i.toDF("ID", i.hashCode.toString) } //the approach may not be necessary
    val joinDF = featuresDF.reduce(_ join (_, "ID"))
    // Create a new column which represents the vector of selected attributes 
    val attributesNames = joinDF.columns.slice(1, joinDF.columns.length)
    val assembler = new VectorAssembler().setInputCols(attributesNames).setOutputCol("features")
    val featuresWithVector = assembler.transform(joinDF)

    // Using the PCA method to transform "features" to a lower dimension "pcaFeatures"
    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(2).fit(featuresWithVector)
    val pcaDF = pca.transform(featuresWithVector).select("ID", "pcaFeatures")
    val partitioner = featuresRDD(0).partitioner.get
    val pcaRdd = pcaDF.map(row => (row.getAs[ID](0), row.getAs[DenseVector](1)))
    val dim1Rdd = pcaRdd.mapValues(v => v.values(0)).sortUnique(partitioner)
    val dim2Rdd = pcaRdd.mapValues(v => v.values(1)).sortUnique(partitioner)

    output(o.attr1, dim1Rdd)
    output(o.attr2, dim2Rdd)
  }
}

