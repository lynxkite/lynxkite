// Reduce multiple double attributes to two double attributes by principle components analysis.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

import org.apache.spark.sql
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArraySeq

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
    val RDDArray: Array[AttributeRDD[Double]] = inputs.features.toArray.map { i => i.rdd }
    val emptyRows = RDDArray(0).mapValues(_ => Array[Double]())
    val featuresRDD: AttributeRDD[Array[Double]] = RDDArray.foldLeft(emptyRows) { (seqs, rdd) =>
      seqs.sortedJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.asInstanceOf[Double] }
    }

    // Using the PCA method to transform "features" to a lower dimension "pcaFeatures"
    val featuresDF = featuresRDD.mapValues(a => Vectors.dense(a)).toDF("ID", "vector")
    val pca = new PCA().setInputCol("vector").setOutputCol("pcaVector").setK(2).fit(featuresDF)
    val pcaDF = pca.transform(featuresDF).select("ID", "pcaVector")
    val partitioner = RDDArray(0).partitioner.get
    val pcaRdd = pcaDF.map(row => (row.getAs[ID](0), row.getAs[DenseVector](1))).sortUnique(partitioner)
    val dim1Rdd = pcaRdd.mapValues(v => v.values(0))
    val dim2Rdd = pcaRdd.mapValues(v => v.values(1))

    output(o.attr1, dim1Rdd)
    output(o.attr2, dim1Rdd)
  }
}

