package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.model
import play.api.libs.json

import scala.reflect.runtime.universe._
import scala.reflect._

class MachineLearningOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.MachineLearningOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  private def typesOf(attrs: List[FEOption], project: ProjectEditor): List[String] = {
    attrs.map(a => project.vertexAttributes(a.id).typeTag.tpe.toString)
  }

  register("Classify with model")(new ProjectTransformation(_) {
    def attrs = project.vertexAttrList[Double] ++ project.vertexAttrList[String]
    def models = project.viewer.models.filter(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the classifications", defaultValue = "prediction"),
      ModelParams("model", "The parameters of the model", models, attrs, typesOf(attrs, project)),
    )
    def enabled =
      FEStatus.assert(models.nonEmpty, "No classification models.") &&
        FEStatus.assert(attrs.nonEmpty, "No numeric or string vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of attribute.")
      assert(params("model").nonEmpty, "Please select a model.")
      val name = params("name")
      val p = json.Json.parse(params("model"))
      val modelName = (p \ "modelName").as[String]
      val modelValue: Scalar[model.Model] = project.scalars(modelName).runtimeSafeCast[model.Model]
      val features = (p \ "features").as[List[String]].map(name => project.vertexAttributes(name))
      val featureTypes = features.map(f => SerializableType(f.typeTag))
      import model.Implicits._
      val generatesProbability = modelValue.modelMeta.generatesProbability
      val isBinary = modelValue.modelMeta.isBinary
      import scala.language.existentials
      val op = graph_operations.ClassifyWithModel(modelValue.modelMeta.labelType, featureTypes)
      val result = op(op.model, modelValue)(op.features, features).result
      val classifiedAttribute = result.classification
      project.newVertexAttribute(name, classifiedAttribute, s"classification according to ${modelName}")
      if (generatesProbability) {
        val certainty = result.probability
        project.newVertexAttribute(
          name + "_certainty",
          certainty,
          s"probability of predicted class according to ${modelName}")
        if (isBinary) {
          val probabilityOf0 = graph_operations.DeriveScala.derive[Double](
            "if (classification == 0) certainty else 1 - certainty",
            Seq("certainty" -> certainty, "classification" -> classifiedAttribute))
          project.newVertexAttribute(
            name + "_probability_of_0",
            probabilityOf0,
            s"probability of class 0 according to ${modelName}")
          val probabilityOf1 = graph_operations.DeriveScala.derive[Double](
            "1 - probabilityOf0",
            Seq("probabilityOf0" -> probabilityOf0))
          project.newVertexAttribute(
            name + "_probability_of_1",
            probabilityOf1,
            s"probability of class 1 according to ${modelName}")
        }
      }
    }
  })

  register("Predict with model")(new ProjectTransformation(_) {
    def attrs = project.vertexAttrList[Double]
    def models = project.viewer.models.filterNot(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the predictions", defaultValue = "prediction"),
      ModelParams("model", "The parameters of the model", models, attrs, typesOf(attrs, project)),
    )
    def enabled =
      FEStatus.assert(models.nonEmpty, "No regression models.") &&
        FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of attribute.")
      assert(params("model").nonEmpty, "Please select a model.")
      val name = params("name")
      val p = json.Json.parse(params("model"))
      val modelName = (p \ "modelName").as[String]
      val modelValue: Scalar[model.Model] = project.scalars(modelName).runtimeSafeCast[model.Model]
      val features = (p \ "features").as[List[String]].map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val predictedAttribute = {
        val op = graph_operations.PredictFromModel(features.size)
        op(op.model, modelValue)(op.features, features).result.prediction
      }
      project.newVertexAttribute(name, predictedAttribute, s"predicted from ${modelName}")
    }
  })

  register("Predict vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("features", "Predictors", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice(
        "method",
        "Method",
        options = FEOption.list(
          "Linear regression",
          "Ridge regression",
          "Lasso",
          "Logistic regression",
          "Naive Bayes",
          "Decision tree",
          "Random forest",
          "Gradient-boosted trees"),
      ),
    )
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val method = params("method").capitalize
      val label = params("label")
      s"$method for $label"
    }
    def apply() = {
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features")
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val method = params("method")
      val prediction = {
        val op = graph_operations.Regression(method, features.size)
        op(op.label, label)(op.features, features).result.prediction
      }
      project.newVertexAttribute(s"${labelName}_prediction", prediction, s"$method for $labelName")
    }
  })

  register("Reduce attribute dimensions")(new ProjectTransformation(_) {
    params ++= List(
      Param("save_as", "Save embedding as", defaultValue = "embedding"),
      Choice("vector", "High-dimensional vector", options = project.vertexAttrList[Vector[Double]]),
      Param("dimensions", "Dimensions", defaultValue = "2"),
      Choice("method", "Embedding method", options = FEOption.list("t-SNE", "PCA")),
      Param("perplexity", "Perplexity", defaultValue = "30", group = "t-SNE options"),
    )
    def enabled = FEStatus.assert(
      project.vertexAttrList[Vector[Double]].nonEmpty,
      "No vector vertex attributes.")
    def apply() = {
      val name = params("save_as")
      assert(name.nonEmpty, "Please set the name of the embedding.")
      val dimensions = params("dimensions").toInt
      val vector = project.vertexAttributes(params("vector")).runtimeSafeCast[Vector[Double]]
      val op = params("method") match {
        case "t-SNE" => graph_operations.TSNE(dimensions, params("perplexity").toDouble)
        case "PCA" => graph_operations.PCA(dimensions)
      }
      project.vertexAttributes(name) = op(op.vector, vector).result.embedding
    }
  })

  register("Split to train and test set")(new ProjectTransformation(_) {
    params ++= List(
      Choice("source", "Source attribute", options = project.vertexAttrList),
      Ratio("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
      RandomSeed("seed", "Random seed for test set selection", context.box),
    )
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val sourceName = params("source")
      val source = project.vertexAttributes(sourceName)
      val roles = {
        val op = graph_operations.CreateRole(
          params("test_set_ratio").toDouble,
          params("seed").toInt)
        op(op.vertices, source.vertexSet).result.role
      }
      val testSetRatio = params("test_set_ratio").toDouble
      val parted = partitionVariable(source, roles)

      project.newVertexAttribute(s"${sourceName}_test", parted.test, s"ratio: $testSetRatio" + help)
      project.newVertexAttribute(s"${sourceName}_train", parted.train, s"ratio: ${1 - testSetRatio}" + help)
    }
    def partitionVariable[T](
        source: Attribute[T],
        roles: Attribute[String]): graph_operations.PartitionAttribute.Output[T] = {
      val op = graph_operations.PartitionAttribute[T]()
      op(op.attr, source)(op.role, roles).result
    }
  })

  register("Train a decision tree classification model")(new ProjectTransformation(_) {
    def attrs = project.vertexAttrList[Double] ++ project.vertexAttrList[String]
    params ++= List(
      Param("name", "The name of the model", defaultValue = "model"),
      Choice("label", "Label", options = attrs),
      Choice("features", "Features", options = attrs, multipleChoice = true),
      Choice("impurity", "Impurity", options = FEOption.list("entropy", "gini")),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed", context.box),
    )
    def enabled =
      FEStatus.assert(attrs.nonEmpty, "No numeric or string vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val labelName = params("label")
      val featureNames = params("features").split(",", -1).sorted
      val label = project.vertexAttributes(labelName)
      val features = featureNames.map {
        name => project.vertexAttributes(name)
      }
      val model = {
        val op = graph_operations.TrainDecisionTreeClassifier(
          labelName = labelName,
          labelType = SerializableType(label.typeTag),
          featureNames = featureNames.toList,
          featureTypes = features.map(f => SerializableType(f.typeTag)).toList,
          impurity = params("impurity"),
          maxBins = params("maxBins").toInt,
          maxDepth = params("maxDepth").toInt,
          minInfoGain = params("minInfoGain").toDouble,
          minInstancesPerNode = params("minInstancesPerNode").toInt,
          seed = params("seed").toInt,
        )
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a decision tree regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model", defaultValue = "model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed", context.box),
    )
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val labelName = params("label")
      val featureNames = params("features").split(",", -1).sorted
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val model = {
        val op = graph_operations.TrainDecisionTreeRegressor(
          labelName = labelName,
          featureNames = featureNames.toList,
          impurity = "variance",
          maxBins = params("maxBins").toInt,
          maxDepth = params("maxDepth").toInt,
          minInfoGain = params("minInfoGain").toDouble,
          minInstancesPerNode = params("minInstancesPerNode").toInt,
          seed = params("seed").toInt,
        )
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a k-means clustering model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model", defaultValue = "model"),
      Choice(
        "features",
        "Attributes",
        options = project.vertexAttrList[Double],
        multipleChoice = true),
      NonNegInt("k", "Number of clusters", default = 2),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20),
      RandomSeed("seed", "Seed", context.box),
    )
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val k = params("k")
      s"Train a k-means clustering model (k=$k)"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val k = params("k").toInt
      val maxIter = params("max_iter").toInt
      val seed = params("seed").toLong
      val model = {
        val op = graph_operations.KMeansClusteringModelTrainer(
          k,
          maxIter,
          seed,
          featureNames.toList)
        op(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train a logistic regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model", defaultValue = "model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20),
      Param("elastic_net_param", "Elastic net mixing", defaultValue = "0.0"),
      Param("reg_param", "Regularization", defaultValue = "0.0"),
    )
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one feature.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val maxIter = params("max_iter").toInt
      val model = {
        val op = graph_operations.LogisticRegressionModelTrainer(
          maxIter,
          labelName,
          featureNames.toList,
          params("elastic_net_param").toDouble,
          params("reg_param").toDouble)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train linear regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model", defaultValue = "model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice(
        "method",
        "Method",
        options = FEOption.list(
          "Linear regression",
          "Ridge regression",
          "Lasso")),
    )
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    override def summary = {
      val method = params("method").capitalize
      val label = params("label")
      s"Build a $method model for $label"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set the name of the model.")
      assert(params("features").nonEmpty, "Please select at least one predictor.")
      val featureNames = splitParam("features").sorted
      val features = featureNames.map {
        name => project.vertexAttributes(name).runtimeSafeCast[Double]
      }
      val name = params("name")
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val method = params("method")
      val model = {
        val op = graph_operations.RegressionModelTrainer(
          method,
          labelName,
          featureNames.toList)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Embed vertices")(new ProjectTransformation(_) {
    params ++= List(
      Param("save_as", "The name of the embedding", defaultValue = "embedding"),
      Param("iterations", "Iterations", defaultValue = "20"),
      Param("dimensions", "Dimensions", defaultValue = "128"),
      Param("walk_length", "Walk length", defaultValue = "20"),
      Param("walks_per_node", "Walks per node", defaultValue = "10"),
      Param("context_size", "Context size", defaultValue = "10"),
    )
    def enabled = project.hasEdgeBundle
    def apply() = {
      val name = params("save_as")
      assert(name.nonEmpty, "Please set the name of the embedding.")
      val dimensions = params("dimensions").toInt
      val iterations = params("iterations").toInt
      val walkLength = params("walk_length").toInt
      val walksPerNode = params("walks_per_node").toInt
      val contextSize = params("context_size").toInt
      val op = graph_operations.Node2Vec(dimensions, iterations, walkLength, walksPerNode, contextSize)
      project.vertexAttributes(name) = op(op.es, project.edgeBundle).result.embedding
    }
  })

  register("Train a GCN classifier")(new ProjectTransformation(_) {
    params ++= List(
      Param("save_as", "Save model as", defaultValue = "model"),
      Param("iterations", "Iterations", defaultValue = "20"),
      Choice("features", "Feature vector", options = project.vertexAttrList[Vector[Double]]),
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("forget", "Use labels as inputs", options = FEOption.bools),
      Param("batch_size", "Batch size", defaultValue = "128"),
      Param("learning_rate", "Learning rate", defaultValue = "0.01"),
      Param("hidden_size", "Hidden size", defaultValue = "16"),
      Param("num_conv_layers", "Number of convolution layers", defaultValue = "2"),
      Choice("conv_op", "Convolution operator", options = FEOption.list("GCNConv", "GatedGraphConv")),
      RandomSeed("seed", "Random seed", context.box),
    )
    def enabled = project.hasEdgeBundle && FEStatus.assert(
      project.vertexAttrList[Double].nonEmpty,
      "No numerical vertex attributes.")
    def apply() = {
      val name = params("save_as")
      assert(name.nonEmpty, "Please set the name of the model.")
      val op = graph_operations.TrainGCNClassifier(
        iterations = params("iterations").toInt,
        forget = params("forget").toBoolean,
        batchSize = params("batch_size").toInt,
        learningRate = params("learning_rate").toDouble,
        numConvLayers = params("num_conv_layers").toInt,
        hiddenSize = params("hidden_size").toInt,
        convOp = params("conv_op"),
        seed = params("seed").toInt,
      )
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features = project.vertexAttributes(params("features")).runtimeSafeCast[Vector[Double]]
      val result = (
        (op(op.es, project.edgeBundle)(
          op.label,
          label)(
          op.features,
          features).result))
      project.newScalar(s"${name}_train_acc", result.trainAcc)
      project.newScalar(s"${name}", result.model)
    }
  })

  register("Train a GCN regressor")(new ProjectTransformation(_) {
    params ++= List(
      Param("save_as", "Save model as", defaultValue = "model"),
      Param("iterations", "Iterations", defaultValue = "20"),
      Choice("features", "Feature vector", options = project.vertexAttrList[Vector[Double]]),
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("forget", "Use labels as inputs", options = FEOption.bools),
      Param("batch_size", "Batch size", defaultValue = "128"),
      Param("learning_rate", "Learning rate", defaultValue = "0.01"),
      Param("hidden_size", "Hidden size", defaultValue = "16"),
      Param("num_conv_layers", "Number of convolution layers", defaultValue = "2"),
      Choice("conv_op", "Convolution operator", options = FEOption.list("GCNConv", "GatedGraphConv")),
      RandomSeed("seed", "Random seed", context.box),
    )
    def enabled = project.hasEdgeBundle && FEStatus.assert(
      project.vertexAttrList[Double].nonEmpty,
      "No numerical vertex attributes.")
    def apply() = {
      val name = params("save_as")
      assert(name.nonEmpty, "Please set the name of the model.")
      val op = graph_operations.TrainGCNRegressor(
        iterations = params("iterations").toInt,
        forget = params("forget").toBoolean,
        batchSize = params("batch_size").toInt,
        learningRate = params("learning_rate").toDouble,
        hiddenSize = params("hidden_size").toInt,
        numConvLayers = params("num_conv_layers").toInt,
        convOp = params("conv_op"),
        seed = params("seed").toInt,
      )
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features = project.vertexAttributes(params("features")).runtimeSafeCast[Vector[Double]]
      val result = (
        (op(op.es, project.edgeBundle)(op.label, label)(op.features, features).result))
      project.newScalar(s"${name}_train_mse", result.trainMSE)
      project.newScalar(s"${name}", result.model)
    }
  })

  register("Predict with GCN")(new ProjectTransformation(_) {
    params ++= List(
      Param("save_as", "Save prediction as", defaultValue = "prediction"),
      Choice("features", "Feature vector", options = project.vertexAttrList[Vector[Double]]),
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("model", "model", options = project.scalarList[SphynxModel]),
    )
    def enabled = project.hasEdgeBundle && FEStatus.assert(
      project.vertexAttrList[Double].nonEmpty,
      "No numerical vertex attributes.")
    def apply() = {
      val name = params("save_as")
      assert(name.nonEmpty, "Please set the name of the prediction.")
      val modelName = params("model")
      val model = project.scalars(modelName).runtimeSafeCast[SphynxModel]
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val op = graph_operations.PredictWithGCN()
      val features = project.vertexAttributes(params("features")).runtimeSafeCast[Vector[Double]]
      val result = op(
        op.es,
        project.edgeBundle)(
        op.features,
        features)(
        op.label,
        label)(
        op.model,
        model).result
      project.vertexAttributes(name) = result.prediction
    }
  })

}
