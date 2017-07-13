package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.model
import play.api.libs.json

class MachineLearningOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.MachineLearningOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Classify with model")(new ProjectTransformation(_) {
    def models = project.viewer.models.filter(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the classifications"),
      ModelParams("model", "The parameters of the model", models, project.vertexAttrList[Double]))
    def enabled =
      FEStatus.assert(models.nonEmpty, "No classification models.") &&
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
      import model.Implicits._
      val generatesProbability = modelValue.modelMeta.generatesProbability
      val isBinary = modelValue.modelMeta.isBinary
      val op = graph_operations.ClassifyWithModel(features.size)
      val result = op(op.model, modelValue)(op.features, features).result
      val classifiedAttribute = result.classification
      project.newVertexAttribute(name, classifiedAttribute,
        s"classification according to ${modelName}")
      if (generatesProbability) {
        val certainty = result.probability
        project.newVertexAttribute(name + "_certainty", certainty,
          s"probability of predicted class according to ${modelName}")
        if (isBinary) {
          val probabilityOf0 = graph_operations.DeriveScala.derive[Double](
            "if (classification == 0) certainty else 1 - certainty",
            Seq("certainty" -> certainty, "classification" -> classifiedAttribute))
          project.newVertexAttribute(name + "_probability_of_0", probabilityOf0,
            s"probability of class 0 according to ${modelName}")
          val probabilityOf1 = graph_operations.DeriveScala.derive[Double](
            "1 - probabilityOf0", Seq("probabilityOf0" -> probabilityOf0))
          project.newVertexAttribute(name + "_probability_of_1", probabilityOf1,
            s"probability of class 1 according to ${modelName}")
        }
      }
    }
  })

  register("Predict with model")(new ProjectTransformation(_) {
    def models = project.viewer.models.filterNot(_._2.isClassification)
    params ++= List(
      Param("name", "The name of the attribute of the predictions"),
      ModelParams("model", "The parameters of the model", models, project.vertexAttrList[Double]))
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

  register("Predict with a neural network (EXPERIMENTAL)")(new ProjectTransformation(_) {
    params ++= List(
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Param("output", "Save as"),
      Choice("features", "Predictors", options = FEOption.unset +: project.vertexAttrList[Double], multipleChoice = true),
      Choice("networkLayout", "Network layout", options = FEOption.list("GRU", "LSTM", "MLP")),
      NonNegInt("networkSize", "Size of the network", default = 3),
      NonNegInt("radius", "Iterations in prediction", default = 3),
      Choice("hideState", "Hide own state", options = FEOption.bools),
      NonNegDouble("forgetFraction", "Forget fraction", defaultValue = "0.0"),
      NonNegDouble("knownLabelWeight", "Weight for known labels", defaultValue = "1.0"),
      NonNegInt("numberOfTrainings", "Number of trainings", default = 50),
      NonNegInt("iterationsInTraining", "Iterations in training", default = 2),
      NonNegInt("subgraphsInTraining", "Subgraphs in training", default = 10),
      NonNegInt("minTrainingVertices", "Minimum training subgraph size", default = 10),
      NonNegInt("maxTrainingVertices", "Maximum training subgraph size", default = 20),
      NonNegInt("trainingRadius", "Radius for training subgraphs", default = 3),
      RandomSeed("seed", "Seed"),
      NonNegDouble("learningRate", "Learning rate", defaultValue = "0.1"))
    def enabled = project.hasEdgeBundle && FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No vertex attributes.")
    def apply() = {
      val labelName = params("label")
      val label = project.vertexAttributes(labelName).runtimeSafeCast[Double]
      val features: Seq[Attribute[Double]] =
        if (params("features") == FEOption.unset.id) Seq()
        else {
          val featureNames = splitParam("features")
          featureNames.map(name => project.vertexAttributes(name).runtimeSafeCast[Double])
        }
      val prediction = {
        val op = graph_operations.PredictViaNNOnGraphV1(
          featureCount = features.length,
          networkSize = params("networkSize").toInt,
          learningRate = params("learningRate").toDouble,
          radius = params("radius").toInt,
          hideState = params("hideState").toBoolean,
          forgetFraction = params("forgetFraction").toDouble,
          trainingRadius = params("trainingRadius").toInt,
          maxTrainingVertices = params("maxTrainingVertices").toInt,
          minTrainingVertices = params("minTrainingVertices").toInt,
          iterationsInTraining = params("iterationsInTraining").toInt,
          subgraphsInTraining = params("subgraphsInTraining").toInt,
          numberOfTrainings = params("numberOfTrainings").toInt,
          knownLabelWeight = params("knownLabelWeight").toDouble,
          seed = params("seed").toInt,
          gradientCheckOn = false,
          networkLayout = params("networkLayout"))
        op(op.edges, project.edgeBundle)(op.label, label)(op.features, features).result.prediction
      }
      project.vertexAttributes(params("output")) = prediction
    }
  })

  register("Predict vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("label", "Attribute to predict", options = project.vertexAttrList[Double]),
      Choice("features", "Predictors", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("method", "Method", options = FEOption.list(
        "Linear regression", "Ridge regression", "Lasso", "Logistic regression", "Naive Bayes",
        "Decision tree", "Random forest", "Gradient-boosted trees")))
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

  register(
    "Reduce vertex attributes to two dimensions")(new ProjectTransformation(_) {
      params ++= List(
        Param("output_name1", "First dimension name", defaultValue = "reduced_dimension1"),
        Param("output_name2", "Second dimension name", defaultValue = "reduced_dimension2"),
        Choice(
          "features", "Attributes",
          options = project.vertexAttrList[Double], multipleChoice = true))
      def enabled = FEStatus.assert(
        project.vertexAttrList[Double].size >= 2, "Less than two vertex attributes.")
      def apply() = {
        val featureNames = splitParam("features").sorted
        assert(featureNames.size >= 2, "Please select at least two attributes.")
        val features = featureNames.map {
          name => project.vertexAttributes(name).runtimeSafeCast[Double]
        }
        val op = graph_operations.ReduceDimensions(features.size)
        val result = op(op.features, features).result
        project.newVertexAttribute(params("output_name1"), result.attr1, help)
        project.newVertexAttribute(params("output_name2"), result.attr2, help)
      }
    })

  register("Split to train and test set")(new ProjectTransformation(_) {
    params ++= List(
      Choice("source", "Source attribute",
        options = project.vertexAttrList),
      Ratio("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
      RandomSeed("seed", "Random seed for test set selection"))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val sourceName = params("source")
      val source = project.vertexAttributes(sourceName)
      val roles = {
        val op = graph_operations.CreateRole(
          params("test_set_ratio").toDouble, params("seed").toInt)
        op(op.vertices, source.vertexSet).result.role
      }
      val testSetRatio = params("test_set_ratio").toDouble
      val parted = partitionVariable(source, roles)

      project.newVertexAttribute(s"${sourceName}_test", parted.test, s"ratio: $testSetRatio" + help)
      project.newVertexAttribute(s"${sourceName}_train", parted.train, s"ratio: ${1 - testSetRatio}" + help)
    }
    def partitionVariable[T](
      source: Attribute[T], roles: Attribute[String]): graph_operations.PartitionAttribute.Output[T] = {
      val op = graph_operations.PartitionAttribute[T]()
      op(op.attr, source)(op.role, roles).result
    }
  })

  register("Train a decision tree classification model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("impurity", "Impurity", options = FEOption.list("entropy", "gini")),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed"))
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
        val op = graph_operations.TrainDecisionTreeClassifier(
          labelName = labelName,
          featureNames = featureNames.toList,
          impurity = params("impurity"),
          maxBins = params("maxBins").toInt,
          maxDepth = params("maxDepth").toInt,
          minInfoGain = params("minInfoGain").toDouble,
          minInstancesPerNode = params("minInstancesPerNode").toInt,
          seed = params("seed").toInt)
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a decision tree regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("maxBins", "Maximum number of bins", default = 32),
      NonNegInt("maxDepth", "Maximum depth of tree", default = 5),
      NonNegDouble("minInfoGain", "Minimum information gain for splits", defaultValue = "0.0"),
      NonNegInt("minInstancesPerNode", "Minimum size of children after splits", default = 1),
      RandomSeed("seed", "Seed"))
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
          seed = params("seed").toInt)
        op(op.label, label)(op.features, features).result.model
      }
      val name = params("name")
      project.scalars(name) = model
    }
  })

  register("Train a k-means clustering model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice(
        "features", "Attributes",
        options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("k", "Number of clusters", default = 2),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20),
      RandomSeed("seed", "Seed"))
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
          k, maxIter, seed, featureNames.toList)
        op(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train a logistic regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      NonNegInt("max_iter", "Maximum number of iterations", default = 20))
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
          maxIter, labelName, featureNames.toList)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })

  register("Train linear regression model")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "The name of the model"),
      Choice("label", "Label", options = project.vertexAttrList[Double]),
      Choice("features", "Features", options = project.vertexAttrList[Double], multipleChoice = true),
      Choice("method", "Method", options = FEOption.list(
        "Linear regression", "Ridge regression", "Lasso")))
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
          method, labelName, featureNames.toList)
        op(op.label, label)(op.features, features).result.model
      }
      project.scalars(name) = model
    }
  })
}
