// Creates a clustering attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object ClusterVerticesByModel extends OpFromJson {
  class Input(numFeatures: Int) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until numFeatures).map {
      i => vertexAttribute[Double](vertices, Symbol(s"feature-$i"))
    }
    val model = scalar[Model]
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val clustering = vertexAttribute[Double](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = ClusterVerticesByModel((j \ "numFeatures").as[Int])
}
import ClusterVerticesByModel._
case class ClusterVerticesByModel(numFeatures: Int)
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
    val model = inputs.model.value
    val RDDArray = inputs.features.toArray.map { v => v.rdd }
    val unscaledRDD = Model.toLinalgVector(RDDArray, inputs.vertices.rdd)
    val scaledRDD = unscaledRDD.mapValues(v => model.featureScaler.get.transform(v))
    // Use KMeans clustering algorithm to transform data to their corresponding cluster labels
    val clustering = model.load(rc.sparkContext).transform(scaledRDD.values)
    val ids = unscaledRDD.keys // We just put back the keys with a zip
    val partitioner = scaledRDD.partitioner.get

    output(
      o.clustering, ids.zip(clustering).asUniqueSortedRDD(partitioner)
    )
  }
}
