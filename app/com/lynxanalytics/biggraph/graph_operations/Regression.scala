// Trains a machine learning model and uses it to generate predictions of an attribute.
//
// MLlib can use native linear algebra packages when properly configured.
// See http://spark.apache.org/docs/latest/mllib-guide.html#dependencies.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.mllib

object Regression extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val label = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val prediction = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = Regression((j \ "method").as[String], (j \ "numFeatures").as[Int])
}
import Regression._
case class Regression(method: String, numFeatures: Int) extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input(numFeatures)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("method" -> method, "numFeatures" -> numFeatures)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vertices = inputs.vertices.rdd
    val labels = inputs.label.rdd
    val vectors = {
      val emptyArrays = vertices.mapValues(l => new Array[Double](numFeatures))
      val numberedFeatures = inputs.features.zipWithIndex
      val fullArrays = numberedFeatures.foldLeft(emptyArrays) {
        case (a, (f, i)) =>
          a.sortedJoin(f.rdd).mapValues {
            case (a, f) => a(i) = f; a
          }
      }
      val unscaled = fullArrays.mapValues(a => new mllib.linalg.DenseVector(a): mllib.linalg.Vector)
      // Must scale the features or we get NaN predictions. (SPARK-1859)
      val scaler = new mllib.feature.StandardScaler().fit(unscaled.values)
      unscaled.mapValues(v => scaler.transform(v))
    }
    val points = labels.sortedJoin(vectors).values.map {
      case (l, v) => new mllib.regression.LabeledPoint(l, v)
    }
    points.cache

    val algorithm = method match {
      case "linear regression" => new mllib.regression.LinearRegressionWithSGD()
      case "ridge regression" => new mllib.regression.RidgeRegressionWithSGD()
      case "lasso" => new mllib.regression.LassoWithSGD()
    }
    val model = algorithm.setIntercept(true).run(points)
    val predictions = model.predict(vectors.values)
    val ids = vectors.keys // We just put back the keys with a zip.
    output(o.prediction, ids.zip(predictions).asUniqueSortedRDD(vectors.partitioner.get))
  }
}
