// A helper class to handle machine learning models.
package com.lynxanalytics.lynxkite.model

import scala.reflect.runtime.universe._

import com.lynxanalytics.lynxkite.graph_util.HadoopFile
import com.lynxanalytics.lynxkite.graph_util.Timestamp
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util._
import org.apache.spark.ml
import org.apache.spark
import play.api.libs.json

import scala.collection.mutable

import org.apache.spark.ml.feature.StringIndexer

object Implicits {
  // Easy access to the ModelMeta class from Scalar[Model].
  implicit class ModelMetaConverter(self: Scalar[Model]) {
    def modelMeta = self.source.operation.asInstanceOf[ModelMeta]
  }
}

// A unified interface for different types of MLlib models.
trait ModelImplementation {
  // A transformation of dataframe with the model.
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame
  def details: String
  // Override this if SQL can be generated for the model.
  def toSQL(
      labelName: Option[String],
      featureNames: List[String],
      featureReverseMappings: Option[Map[Int, Map[Double, String]]],
      labelReverseMapping: Option[Map[Double, String]]): String =
    "Sorry, SQL generation is not supported for this model type."
}

// Helper classes to provide a common abstraction for various types of models.
private[lynxkite] class LinearRegressionModelImpl(
    m: ml.regression.LinearRegressionModel,
    statistics: String)
    extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
}

private[lynxkite] class DecisionTreeRegressionModelImpl(
    m: ml.regression.DecisionTreeRegressionModel,
    statistics: String)
    extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
}

private[lynxkite] class LogisticRegressionModelImpl(
    m: ml.classification.LogisticRegressionModel,
    statistics: String)
    extends ModelImplementation {
  // Transform the data with logistic regression model to a dataframe with the schema [vector |
  // rawPredition | probability | prediction].
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
  def getThreshold: Double = m.getThreshold
}

private[lynxkite] class ClusterModelImpl(
    m: ml.clustering.KMeansModel,
    statistics: String)
    extends ModelImplementation {
  // Transform the data with clustering model.
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = s"cluster centers: ${m.clusterCenters}\n" + statistics
}

private[lynxkite] class DecisionTreeClassificationModelImpl(
    m: ml.classification.DecisionTreeClassificationModel,
    statistics: String)
    extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics

  import org.apache.spark.ml.tree._
  override def toSQL(
      labelName: Option[String],
      featureNames: List[String],
      featureReverseMappings: Option[Map[Int, Map[Double, String]]],
      labelReverseMapping: Option[Map[Double, String]]): String = {
    val caseStr = printNode(m.rootNode, featureNames, featureReverseMappings, labelReverseMapping, 0)
    val alias = labelName.map(s => s" AS $s").getOrElse("")
    s"${caseStr}${alias}"
  }

  private def printNode(
      node: Node,
      featureNames: List[String],
      featureRM: Option[Map[Int, Map[Double, String]]],
      labelRM: Option[Map[Double, String]],
      indent: Int): String = {
    val indentStr = " " * indent
    node match {
      case n: InternalNode =>
        val leftStr = printNode(n.leftChild, featureNames, featureRM, labelRM, indent + 2)
        val rightStr = printNode(n.rightChild, featureNames, featureRM, labelRM, indent + 2)
        n.split match {
          case s: ContinuousSplit =>
            val feature = featureNames(s.featureIndex)
            s"""${indentStr}CASE
${indentStr} WHEN $feature <= ${s.threshold} THEN
$leftStr
${indentStr} ELSE
$rightStr
${indentStr}END"""
          case s: CategoricalSplit =>
            val feature = featureNames(s.featureIndex)
            val leftCategoriesStr =
              if (featureRM.nonEmpty && featureRM.get.contains(s.featureIndex)) {
                s.leftCategories.map { category =>
                  featureRM.get(s.featureIndex)(category)
                }.mkString("'", "', '", "'") // 'a', 'b', 'c' ...
              } else {
                s.leftCategories.mkString(", ") // 0.0, 1.0, 2.0 ...
              }
            s"""${indentStr}CASE
${indentStr} WHEN $feature IN (${leftCategoriesStr}) THEN
$leftStr
${indentStr} ELSE
$rightStr
${indentStr}END"""
        }
      case n: LeafNode =>
        val prediction = labelRM.map(mapping => s"'${mapping(n.prediction)}'") // 'a'
          .getOrElse(n.prediction) // 1.0
        s"${indentStr}${prediction}"
    }
  }
}

case class Model(
    method: String, // The training method used to create this model.
    symbolicPath: String, // The symbolic name of the HadoopFile where this model is saved.
    labelName: Option[String], // Name of the label attribute used to train this model.
    labelType: Option[SerializableType[_]] = None, // The type of the predicted label.
    // Optional mapping from the model output (always Double) to the correct label type.
    labelReverseMapping: Option[Map[Double, String]] = None,
    featureNames: List[String], // The name of the feature attributes used to train this model.
    // The type of the model features in the same order as the featureNames.
    featureTypes: Option[List[SerializableType[_]]] = None,
    // Mappings for non Double features from their type to Double, identified by the feature's index.
    featureMappings: Option[Map[Int, Map[String, Double]]] = None,
    statistics: Option[String]) // For the details that require training data
    extends ToJson with Equals {

  override def equals(other: Any) = {
    if (canEqual(other)) {
      val o = other.asInstanceOf[Model]
      method == o.method &&
      symbolicPath == o.symbolicPath &&
      labelName == o.labelName &&
      featureNames == o.featureNames
    } else {
      false
    }
  }

  override def canEqual(other: Any) = other.isInstanceOf[Model]

  override def toJson: json.JsValue = {
    json.Json.obj(
      "method" -> method,
      "symbolicPath" -> symbolicPath,
      "labelName" -> labelName,
      "labelType" -> labelType.map(_.toJson),
      "labelReverseMapping" -> labelReverseMapping.map(_.map { case (k, v) => k.toString -> v }),
      "featureNames" -> featureNames,
      "featureTypes" -> featureTypes.map(_.map(_.toJson)),
      "featureMappings" -> featureMappings.map(_.map { case (k, v) => k.toString -> v }),
      "statistics" -> statistics,
    )
  }

  // Loads the previously created model from the file system.
  def load(sc: spark.SparkContext): ModelImplementation = {
    val path = HadoopFile(symbolicPath).resolvedName
    method match {
      case "Linear regression" | "Ridge regression" | "Lasso" =>
        new LinearRegressionModelImpl(ml.regression.LinearRegressionModel.load(path), statistics.get)
      case "Decision tree regression" =>
        new DecisionTreeRegressionModelImpl(ml.regression.DecisionTreeRegressionModel.load(path), statistics.get)
      case "Logistic regression" =>
        new LogisticRegressionModelImpl(ml.classification.LogisticRegressionModel.load(path), statistics.get)
      case "KMeans clustering" =>
        new ClusterModelImpl(ml.clustering.KMeansModel.load(path), statistics.get)
      case "Decision tree classification" =>
        new DecisionTreeClassificationModelImpl(
          ml.classification.DecisionTreeClassificationModel.load(path),
          statistics.get)
    }
  }

  def toSQL(sc: spark.SparkContext): String = {
    load(sc).toSQL(
      labelName,
      featureNames,
      featureMappings.map(_.mapValues(m => m.map(_.swap))),
      labelReverseMapping)
  }
}

object Model extends FromJson[Model] {
  override def fromJson(j: json.JsValue): Model = {
    Model(
      (j \ "method").as[String],
      (j \ "symbolicPath").as[String],
      (j \ "labelName").asOpt[String],
      (j \ "labelType").asOpt[JsValue].filter(_ != json.JsNull).map(json => SerializableType.fromJson(json)),
      (j \ "labelReverseMapping").asOpt[Map[String, String]].map(_.map { case (k, v) => k.toDouble -> v }),
      (j \ "featureNames").as[List[String]],
      (j \ "featureTypes").asOpt[List[JsValue]].map(_.map(json => SerializableType.fromJson(json))),
      (j \ "featureMappings").asOpt[Map[String, Map[String, Double]]].map(_.map { case (k, v) => k.toInt -> v }),
      (j \ "statistics").asOpt[String],
    )
  }
  def toMetaFE(modelName: String, modelMeta: ModelMeta): FEModelMeta = FEModelMeta(
    modelName,
    modelMeta.isClassification,
    modelMeta.generatesProbability,
    modelMeta.featureNames,
    modelMeta.featureTypes.map(_.typeTag.tpe.toString))

  def toFE(m: Model, sc: spark.SparkContext): FEModel = {
    val modelImpl = m.load(sc)
    val featureTypes =
      m.featureTypes.map(_.map(_.typeTag.toString)).getOrElse(m.featureNames.map(_ => "Double"))
    FEModel(
      method = m.method,
      labelName = m.labelName,
      featureNames = m.featureNames,
      featureTypes = featureTypes,
      details = modelImpl.details,
      sql = modelImpl.toSQL(
        m.labelName,
        m.featureNames,
        m.featureMappings.map(_.mapValues(m => m.map(_.swap))),
        m.labelReverseMapping),
    )
  }

  def newModelFile: HadoopFile = {
    HadoopFile("DATA$") / io.ModelsDir / Timestamp.toString
  }

  // Converts the input attributes of various types to a DataFrame of Doubles. Returns a
  // mapping by index for each non-Double attribute from their type to Double.
  def toDoubleDF(
      sqlContext: spark.sql.SQLContext,
      vertices: VertexSetRDD,
      attrsArray: Array[AttributeData[_]])(
      implicit dataSet: DataSet): (spark.sql.DataFrame, Map[Int, Map[String, Double]]) = {
    val mappingsCollector = mutable.Map[Int, Map[String, Double]]()
    val df = toDF(
      sqlContext,
      vertices,
      attrsArray.zipWithIndex.map {
        case (attr, i) =>
          val (rdd, mapping) = toDoubleRDD(attr)
          if (mapping.nonEmpty) {
            mappingsCollector(i) = mapping.get
          }
          rdd
      },
    )
    val mappings = mappingsCollector.toMap
    (markCategoricalVariablesInDFSchema(df, attrsArray.size, mappings), mappings)
  }

  // Converts the input attribute of a certain type to an RDD of Doubles. Optionally returns a
  // mapping for the attribute from its type - if not Double - to Double.
  def toDoubleRDD(
      attr: AttributeData[_])(
      implicit dataSet: DataSet): (AttributeRDD[Double], Option[Map[String, Double]]) = {
    attr match {
      case a if a.is[Double] => (a.runtimeSafeCast[Double].rdd, None)
      case a if a.is[String] =>
        val rdd = a.runtimeSafeCast[String].rdd
        val mapping = rdd.values.distinct.collect.sorted.zipWithIndex.map { case (k, v) => k -> v.toDouble }.toMap
        (rdd.mapValues(v => mapping(v)), Some(mapping))
      case _ => throw new AssertionError()
    }
  }

  // Converts the input attributes of various types to a DataFrame of Doubles. Mappings for non-Double
  // attributes have to be provided.
  def toDF(
      sqlContext: spark.sql.SQLContext,
      vertices: VertexSetRDD,
      attrsArray: Array[AttributeData[_]],
      mappings: Map[Int, Map[String, Double]])(
      implicit dataSet: DataSet): spark.sql.DataFrame = {
    markCategoricalVariablesInDFSchema(
      toDF(
        sqlContext,
        vertices,
        attrsArray.zipWithIndex.map {
          case (attr, i) => attr match {
              case a if a.is[Double] => a.runtimeSafeCast[Double].rdd
              case a if a.is[String] =>
                val rdd = a.runtimeSafeCast[String].rdd
                val mapping = mappings(i)
                rdd.mapValues(v => mapping(v))
              case _ => throw new AssertionError()
            }
        },
      ),
      attrsArray.size,
      mappings,
    )
  }

  // Returns the same DataFrame if mappings is empty. Otherwise creates a new DataFrame with the
  // same values, but with new metadata for the "features" vector column. Every index of the
  // vector (corresponding to a feature) which is present in mapping is made a NominalAttribute.
  // The rest is made a NumericAttribute, equivalent to the original case.
  // This information is used by the ML algorithms to identify features as categorical (nominal) or
  // numerical features.
  private def markCategoricalVariablesInDFSchema(
      df: spark.sql.DataFrame,
      numFeatures: Int,
      mappings: Map[Int, Map[String, Double]]) = {
    if (mappings.isEmpty) {
      df
    } else {
      import org.apache.spark.ml.attribute._
      val newField = {
        val featureAttributes: Array[Attribute] =
          (0 until numFeatures).map {
            i =>
              if (mappings.contains(i)) {
                NominalAttribute
                  .defaultAttr
                  .withIndex(i)
                  .withValues(mappings(i).keys.toArray)
              } else {
                NumericAttribute
                  .defaultAttr
                  .withIndex(i)
              }
          }.toArray
        val newAttributeGroup = new AttributeGroup("features", featureAttributes)
        newAttributeGroup.toStructField()
      }
      df.withColumn("features", df("features").as("features", newField.metadata))
    }
  }

  // Transforms features to an MLlib DataFrame with "id" and "features" columns.
  def toDF(
      sqlContext: spark.sql.SQLContext,
      vertices: VertexSetRDD,
      featuresArray: Array[AttributeRDD[Double]]): spark.sql.DataFrame = {
    val emptyArrays = vertices.mapValues(l => new Array[Double](featuresArray.size))
    val numberedFeatures = featuresArray.zipWithIndex
    val fullArrays = numberedFeatures.foldLeft(emptyArrays) {
      case (a, (f, i)) =>
        a.sortedJoin(f).mapValues {
          case (a, f) => a(i) = f; a
        }
    }
    val featureRDD = fullArrays.mapValues(a => new ml.linalg.DenseVector(a): ml.linalg.Vector)
    import sqlContext.implicits._
    featureRDD.toDF("id", "features")
  }

  def getMAPE(predictionAndLabels: spark.sql.DataFrame): Double = {
    predictionAndLabels.rdd.map {
      row =>
        {
          val prediction = row.getDouble(0)
          val label = row.getDouble(1)
          if (prediction == label) {
            0.0
            // Return an error of 100% if a zero division error occurs.
          } else if (prediction == 0.0) {
            1.0
          } else {
            math.abs(prediction / label - 1.0)
          }
        }
    }.mean * 100.0
  }
}

// Helper method to print statistical tables of the models.
object Tabulator {
  def getTable(
      headers: Array[String],
      rowNames: Array[String],
      columnData: Array[Array[Double]]): String = {
    assert(
      rowNames.size == columnData(0).size,
      s"Size mismatch: rowNames (${rowNames.size}) != columnData[0] (${columnData(0).size})")
    val tails = rowNames +: columnData.map(_.map(x => f"$x%1.6f"))
    assert(
      headers.size == tails.size,
      s"Size mismatch: headers (${headers.size}) != 1 + columnData (${columnData.size})")
    format(headers +: tails.transpose)
  }

  def format(table: Array[Array[String]]): String = {
    val colSizes = table.transpose.map(_.map(_.length).max)
    val dataAndSizes = table.map(_.zip(colSizes))
    dataAndSizes.map {
      row =>
        row.map {
          case (data, size) => ("%" + size + "s").format(data)
        }.mkString("  ")
    }.mkString("\n")
  }
}

case class FEModelMeta(
    name: String,
    isClassification: Boolean,
    generatesProbability: Boolean,
    featureNames: List[String],
    featureTypes: List[String])

case class FEModel(
    method: String,
    labelName: Option[String],
    featureNames: List[String],
    featureTypes: List[String],
    details: String,
    sql: String)

trait ModelMeta {
  def isClassification: Boolean
  def isBinary: Boolean
  def generatesProbability: Boolean = false
  def labelType: SerializableType[_]
  def featureNames: List[String]
  def featureTypes: List[SerializableType[_]]
}
