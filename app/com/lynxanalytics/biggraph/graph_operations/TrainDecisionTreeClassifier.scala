// Trains a decision tree classification model.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect._
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import org.apache.spark.ml
import org.apache.spark.sql

object TrainDecisionTreeClassifier extends OpFromJson {
  class Input(labelType: SerializableType[_], featureTypes: Seq[SerializableType[_]]) extends MagicInputSignature {
    val vertices = vertexSet
    val features = (0 until featureTypes.size).map {
      i => runtimeTypedVertexAttribute(vertices, Symbol(s"feature-$i"), featureTypes(i).typeTag)
    }
    val label = runtimeTypedVertexAttribute(vertices, 'label, labelType.typeTag)
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val model = scalar[Model]
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    val doubleType = SerializableType.double
    val featureNames = (j \ "featureNames").as[List[String]]
    TrainDecisionTreeClassifier(
      (j \ "labelName").as[String],
      (j \ "labelType").asOpt[JsValue].map(SerializableType.fromJson(_)).getOrElse(doubleType),
      featureNames,
      (j \ "featureTypes").asOpt[List[JsValue]]
        .map(_.map(json => SerializableType.fromJson(json)))
        .getOrElse(featureNames.map(_ => doubleType)),
      (j \ "impurity").as[String],
      (j \ "maxBins").as[Int],
      (j \ "maxDepth").as[Int],
      (j \ "minInfoGain").as[Double],
      (j \ "minInstancesPerNode").as[Int],
      (j \ "seed").as[Int],
    )
  }
}

import TrainDecisionTreeClassifier._
case class TrainDecisionTreeClassifier[T: TypeTag](
    labelName: String,
    labelType: SerializableType[T],
    featureNames: List[String],
    featureTypes: List[SerializableType[_]],
    impurity: String,
    maxBins: Int,
    maxDepth: Int,
    minInfoGain: Double,
    minInstancesPerNode: Int,
    seed: Int)
    extends SparkOperation[Input, Output] with ModelMeta {
  val isClassification = true
  val isBinary = false
  override val generatesProbability = true
  override val isHeavy = true
  @transient override lazy val inputs = new Input(labelType, featureTypes)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "labelName" -> labelName,
    "featureNames" -> featureNames,
    "impurity" -> impurity,
    "maxBins" -> maxBins,
    "maxDepth" -> maxDepth,
    "minInfoGain" -> minInfoGain,
    "minInstancesPerNode" -> minInstancesPerNode,
    "seed" -> seed,
  ) ++
    // Store types only if different from the default.
    // This avoids a GUID change on operations saved before typing was added.
    (if (labelType == SerializableType.double) Json.obj()
     else Json.obj(
       "labelType" -> labelType.toJson)) ++
    (if (featureTypes.forall(_ == SerializableType.double)) Json.obj()
     else Json.obj(
       "featureTypes" -> featureTypes.map(f => f.toJson)))

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.sparkDomain.newSQLContext()
    import sqlContext.implicits._

    val (labelRDD, labelMapping) = Model.toDoubleRDD(inputs.label.data)
    val labelDF = labelRDD.toDF("id", "label")
    val (featuresDF, featureMappings) =
      Model.toDoubleDF(sqlContext, inputs.vertices.rdd, inputs.features.toArray.map(_.data))
    val labeledFeaturesDF = featuresDF.join(labelDF, "id")
    assert(!labeledFeaturesDF.rdd.isEmpty, "Training is not possible with empty data set.")

    val decisionTreeClassifier = new ml.classification.DecisionTreeClassifier()
      .setRawPredictionCol("rawClassification")
      .setPredictionCol("classification")
      .setProbabilityCol("probability")
      .setImpurity(impurity)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setSeed(seed)
    val model = decisionTreeClassifier.fit(labeledFeaturesDF)
    val file = Model.newModelFile
    model.save(file.resolvedName)
    val treeDescription = (0 until featureNames.length).foldLeft { model.toDebugString } {
      (description, i) => description.replaceAll("feature " + i.toString, featureNames(i))
    }.replaceFirst("uid=\\w*, ", "")
    val prediction = model.transform(labeledFeaturesDF)
    val evaluator = new ml.evaluation.MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("classification")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(prediction).toString
    val dataSize = labelDF.count().toDouble
    val support = labelDF.groupBy("label").count().orderBy(sortCol = "label").map(row =>
      (row.getAs[Long]("count").toDouble / dataSize)).collectAsList()
    val statistics = s"$treeDescription\nAccuracy: $accuracy\nSupport: $support"
    output(
      o.model,
      Model(
        method = "Decision tree classification",
        symbolicPath = file.symbolicName,
        labelName = Some(labelName),
        labelType = Some(labelType),
        labelReverseMapping = labelMapping.map(_.map { case (k, v) => v -> k }.toMap),
        featureNames = featureNames,
        featureTypes = Some(featureTypes),
        featureMappings = Some(featureMappings),
        statistics = Some(statistics),
      ),
    )
  }
}
