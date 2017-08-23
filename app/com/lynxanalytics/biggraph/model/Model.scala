// A helper class to handle machine learning models.
package com.lynxanalytics.biggraph.model

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._
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
  def toSQL(labelName: Option[String], featureNames: List[String]): String = ""
}

// Helper classes to provide a common abstraction for various types of models.
private[biggraph] class LinearRegressionModelImpl(
    m: ml.regression.LinearRegressionModel,
    statistics: String) extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
}

private[biggraph] class DecisionTreeRegressionModelImpl(
    m: ml.regression.DecisionTreeRegressionModel,
    statistics: String) extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
}

private[biggraph] class LogisticRegressionModelImpl(
    m: ml.classification.LogisticRegressionModel,
    statistics: String) extends ModelImplementation {
  // Transform the data with logistic regression model to a dataframe with the schema [vector |
  // rawPredition | probability | prediction].
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics
  def getThreshold: Double = m.getThreshold
}

private[biggraph] class ClusterModelImpl(
    m: ml.clustering.KMeansModel, statistics: String) extends ModelImplementation {
  // Transform the data with clustering model.
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = s"cluster centers: ${m.clusterCenters}\n" + statistics
}

private[biggraph] class DecisionTreeClassificationModelImpl(
    m: ml.classification.DecisionTreeClassificationModel,
    statistics: String) extends ModelImplementation {
  def transformDF(data: spark.sql.DataFrame): spark.sql.DataFrame = m.transform(data)
  def details: String = statistics

  import org.apache.spark.ml.tree._
  override def toSQL(labelName: Option[String], featureNames: List[String]): String = {
    val caseStr = printNode(m.rootNode, featureNames, 0)
    val alias = labelName.map(s => s" AS $s").getOrElse("")
    s"${caseStr}${alias}"
  }

  private def printNode(node: Node, featureNames: List[String], indent: Int): String = {
    node match {
      case n: InternalNode =>
        n.split match {
          case s: ContinuousSplit =>
            val feature = featureNames(s.featureIndex)
            val leftStr = printNode(n.leftChild, featureNames, indent + 2)
            val rightStr = printNode(n.rightChild, featureNames, indent + 2)
            val indentStr = " " * indent
            s"""${indentStr}CASE
${indentStr} WHEN $feature <= ${s.threshold} THEN
$leftStr
${indentStr} ELSE
$rightStr
${indentStr}END"""
          case s: CategoricalSplit => throw new AssertionError("CategoricalSplit is not supported.")
        }
      case n: LeafNode =>
        val indentStr = " " * indent
        s"${indentStr}${n.prediction}"
    }
  }
}

case class Model(
  method: String, // The training method used to create this model.
  symbolicPath: String, // The symbolic name of the HadoopFile where this model is saved.
  labelName: Option[String], // Name of the label attribute used to train this model.
  labelType: Option[String] = None,
  labelMapping: Option[Map[_, Double]] = None,
  featureNames: List[String], // The name of the feature attributes used to train this model.
  featureTypes: Option[List[String]] = None,
  featureMappings: Option[Map[String, Map[_, Double]]] = None,
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
      "labelType" -> labelType,
      "labelMapping" -> labelMapping,
      "featureNames" -> featureNames,
      "featureTypes" -> featureTypes,
      "featureMappings" -> featureMappings,
      "statistics" -> statistics
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
          ml.classification.DecisionTreeClassificationModel.load(path), statistics.get)
    }
  }

  def toSQL(sc: spark.SparkContext): String = {
    load(sc).toSQL(labelName, featureNames)
  }
}

object Model extends FromJson[Model] {
  override def fromJson(j: json.JsValue): Model = {
    Model(
      (j \ "method").as[String],
      (j \ "symbolicPath").as[String],
      (j \ "labelName").as[Option[String]],
      (j \ "labelType").as[Option[String]],
      (j \ "labelConverter").as[Option[Map[_, Double]]],
      (j \ "featureNames").as[List[String]],
      (j \ "featureTypes").as[Option[List[String]]],
      (j \ "featureMappings").as[Option[Map[String, Map[_, Double]]]],
      (j \ "statistics").as[Option[String]]
    )
  }
  def toMetaFE(modelName: String, modelMeta: ModelMeta): FEModelMeta = FEModelMeta(
    modelName, modelMeta.isClassification, modelMeta.generatesProbability, modelMeta.featureNames)

  def toFE(m: Model, sc: spark.SparkContext): FEModel = {
    val modelImpl = m.load(sc)
    FEModel(
      method = m.method,
      labelName = m.labelName,
      featureNames = m.featureNames,
      details = modelImpl.details,
      sql = modelImpl.toSQL(m.labelName, m.featureNames))
  }

  def newModelFile: HadoopFile = {
    HadoopFile("DATA$") / io.ModelsDir / Timestamp.toString
  }

  def toDoubleDF(
    sqlContext: spark.sql.SQLContext,
    vertices: VertexSetRDD,
    attrsArray: Array[com.lynxanalytics.biggraph.graph_api.MagicInputSignature#RuntimeTypedVATemplate],
    mappingsCollector: mutable.Map[String, Map[_, Double]])(
      implicit dataSet: DataSet): spark.sql.DataFrame = {
    toDF(sqlContext, vertices, attrsArray.map { attr =>
      val (rdd, mapping) = toDoubleRDD(attr)
      if (mapping.nonEmpty) {
        mappingsCollector(attr.name.name) = mapping.get
      }
      rdd
    })
  }

  def toDoubleRDD(
    attr: com.lynxanalytics.biggraph.graph_api.MagicInputSignature#RuntimeTypedVATemplate)(
      implicit dataSet: DataSet): (AttributeRDD[Double], Option[Map[_, Double]]) = {
    attr match {
      case f if f.tt.tpe =:= typeOf[Double] => (f.rdd.mapValues(v => v.asInstanceOf[Double]), None)
      case f if f.tt.tpe =:= typeOf[String] =>
        val rdd = f.rdd.mapValues(v => v.asInstanceOf[String])
        val mapping = rdd.values.distinct.collect.sorted.zipWithIndex.map { case (k, v) => k -> v.toDouble }.toMap
        (rdd.mapValues(v => mapping(v)), Some(mapping))
      case _ => throw new AssertionError()
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
    assert(rowNames.size == columnData(0).size,
      s"Size mismatch: rowNames (${rowNames.size}) != columnData[0] (${columnData(0).size})")
    val tails = rowNames +: columnData.map(_.map(x => f"$x%1.6f"))
    assert(headers.size == tails.size,
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
  featureNames: List[String])

case class FEModel(
  method: String,
  labelName: Option[String],
  featureNames: List[String],
  details: String,
  sql: String)

trait ModelMeta {
  def isClassification: Boolean
  def isBinary: Boolean
  def generatesProbability: Boolean = false
  def featureNames: List[String]
}
