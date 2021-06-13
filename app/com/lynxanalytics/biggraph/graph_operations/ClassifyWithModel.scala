// Creates a classification attribute from a machine learning model.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model.Model
import com.lynxanalytics.biggraph.model.Implicits._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark.ml.linalg.DenseVector

import scala.reflect.runtime.universe._
import scala.reflect._

object ClassifyWithModel extends OpFromJson {
  class Input(featureTypes: Seq[SerializableType[_]]) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until featureTypes.size).map {
      i => runtimeTypedVertexAttribute(vertices, Symbol(s"feature-$i"), featureTypes(i).typeTag)
    }
    val model = scalar[Model]
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val probability = {
      val modelMeta = inputs.model.entity.modelMeta
      if (modelMeta.generatesProbability) {
        vertexAttribute[Double](inputs.vertices.entity)
      } else { null }
    }
    val classification = vertexAttribute[T](inputs.vertices.entity)
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    (j \ "numFeatures").asOpt[Int]
      .map(numFeatures =>
        // Compatibility with operations saved before typing was added.
        ClassifyWithModel[Double](
          SerializableType.double,
          (1 to numFeatures).map(_ => SerializableType.double).toList))
      .getOrElse(
        // "Any" is used to effectively disable type checks. (This generally happens by itself in
        // "fromJson" methods when we just don't specify a type parameter, but here we need to align
        // the "labelType" parameter with the type parameter.)
        ClassifyWithModel[Any](
          SerializableType.fromJson(j \ "labelType").asInstanceOf[SerializableType[Any]],
          (j \ "featureTypes").as[List[JsValue]].map(json => SerializableType.fromJson(json)),
        ))
  }
}
import ClassifyWithModel._
case class ClassifyWithModel[T](
    labelType: SerializableType[T],
    featureTypes: List[SerializableType[_]])
    extends SparkOperation[Input, Output[T]] {
  @transient override lazy val inputs = new Input(featureTypes)
  override val isHeavy = true
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output[T]()(labelType.typeTag, instance, inputs)
  override def toJson =
    if (labelType == SerializableType.double && featureTypes.forall(_ == SerializableType.double))
      // Store types only if different from the default.
      // This avoids a GUID change on operations saved before typing was added.
      Json.obj("numFeatures" -> featureTypes.size)
    else
      Json.obj(
        "labelType" -> labelType.toJson,
        "featureTypes" -> featureTypes.map(f => f.toJson))

  def execute(
      inputDatas: DataSet,
      o: Output[T],
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = labelType.classTag
    val sqlContext = rc.sparkDomain.newSQLContext()
    import sqlContext.implicits._

    val modelValue = inputs.model.value
    val partitioner = inputs.vertices.rdd.partitioner.get
    val classificationModel = modelValue.load(rc.sparkContext)
    val inputDF = Model.toDF(
      sqlContext,
      inputs.vertices.rdd,
      inputs.features.toArray.map(_.data),
      modelValue.featureMappings.getOrElse(Map()))

    // Transform data to an attributeRDD with the attribute (probability, classification)
    val transformation = classificationModel.transformDF(inputDF)
    val labelReverseMapping = modelValue.labelReverseMapping
    val classification = transformation.select("ID", "classification").map { row =>
      (row.getAs[ID]("ID"), row.getAs[java.lang.Number]("classification").doubleValue)
    }.rdd
      .mapValues {
        v => { if (labelReverseMapping.nonEmpty) labelReverseMapping.get(v) else v }.asInstanceOf[T]
      }
      .sortUnique(partitioner)
    // Output the probability corresponded to the classification labels.
    if (o.probability != null) {
      val probability = transformation.select("ID", "probability", "classification").map { row =>
        val classification = row.getAs[Double]("classification").toInt
        val probability = row.getAs[DenseVector]("probability")(classification)
        (row.getAs[ID]("ID"), probability)
      }.rdd.sortUnique(partitioner)
      output(o.probability, probability)
    }
    output(o.classification, classification)
  }
}
