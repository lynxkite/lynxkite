package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ClassifyWithModelTest extends ModelTestBase {
  test("test the k-means clustering model on larger data set with 20 attributes") {
    val numAttr = 20
    val numData = 100
    val k = 10
    val m = testKMeansModel(numAttr, numData, k) // From the ModelTestBase class.

    // A 4 vertices graph where the first data point has 20 attributes of value 100.0,
    // the second data point has 20 attributes of value 96.0, the third data point has
    // 20 attributes of value 5.0 and the last data point has 20 attributes of 1.0.
    val g = graph(numVertices = 4)
    val attrs = (0 until numAttr).map(_ => Map(0 -> 100.0, 1 -> 96.0, 2 -> 5.0, 3 -> 1.0))
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ClassifyWithModel(
      SerializableType.double,
      (0 until numAttr).map(_ => SerializableType.double).toList)
    val result = op(op.features, features)(op.model, m).result
    val clustering = result.classification.rdd.values.collect
    assert(clustering.size == 4)
    // Check that the first two and the last two data points shall have identical labels.
    assert(clustering(0) == clustering(1))
    assert(clustering(2) == clustering(3))
    // Check that distant data points have different labels.
    assert(clustering(0) != clustering(3))
  }

  test("test the logistic regression model") {
    val m = model(
      method = "Logistic regression",
      labelName = "isSenior",
      label = Map(0 -> 0, 1 -> 1, 2 -> 0, 3 -> 1),
      featureNames = List("age"),
      attrs = Seq(Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60)),
      graph(4),
    )

    val g = graph(numVertices = 4)
    val attrs = (0 until 1).map(_ => Map(0 -> 15.0, 1 -> 20.0, 2 -> 50.0, 3 -> 60.0))
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ClassifyWithModel(SerializableType.double, List(SerializableType.double))
    val result = op(op.features, features)(op.model, m).result
    val classification = result.classification.rdd.values.collect
    assert(classification.size == 4)
    // Check data points with similar ages have the same label.
    assert(classification(0) == 0.0 && classification(1) == 0.0)
    assert(classification(2) == 1.0 && classification(3) == 1.0)
    val probability = result.probability.rdd.values.collect
    // Check that the probability is higher if the datapoint is farther from the threshold.
    assert(probability(0) > probability(1))
    assert(probability(3) > probability(2))

  }

  test("test the decision tree classification model") {
    import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests.{
      trainingData,
      testDataForClassification,
    }
    val m = model(
      method = "Decision tree classification",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber),
    )

    val g = graph(testDataForClassification.vertexNumber)
    val attrs = testDataForClassification.attrs
    val features = attrs.map(attr => {
      AddVertexAttribute.run[Double](g.vs, attr.mapValues(_.asInstanceOf[Double]))
    })
    val op = ClassifyWithModel(
      SerializableType.double,
      (0 until testDataForClassification.featureNames.size).map(_ => SerializableType.double).toList)
    val result = op(op.features, features)(op.model, m).result
    val classification = result.classification.rdd.collect.toMap
    assert(classification.size == testDataForClassification.vertexNumber)
    assert(classification == testDataForClassification.label)
    val probability = result.probability.rdd.collect.toMap
    assertRoughlyEquals(probability, testDataForClassification.probability, 0.1)
  }

  test("test the decision tree classification model - string attributes") {
    import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests.{
      typedTrainingData,
      typedTestDataForClassification,
    }
    val m = {
      val g = graph(typedTrainingData.vertexNumber)
      val l = AddVertexAttribute.run(g.vs, typedTrainingData.label)
      val features =
        typedTrainingData.stringAttrs.map(attr => AddVertexAttribute.run(g.vs, attr)) ++
          typedTrainingData.doubleAttrs.map(attr => AddVertexAttribute.run(g.vs, attr))
      val op = TrainDecisionTreeClassifier(
        typedTrainingData.labelName,
        SerializableType.string,
        typedTrainingData.featureNames,
        List(SerializableType.string, SerializableType.double),
        impurity = "gini",
        maxBins = 32,
        maxDepth = 5,
        minInfoGain = 0,
        minInstancesPerNode = 1,
        seed = 1234567,
      )
      op(op.features, features)(op.label, l).result.model
    }

    val g = graph(typedTestDataForClassification.vertexNumber)
    val features =
      typedTestDataForClassification.stringAttrs.map(attr => AddVertexAttribute.run(g.vs, attr)) ++
        typedTestDataForClassification.doubleAttrs.map(attr => AddVertexAttribute.run(g.vs, attr))
    val op = ClassifyWithModel[String](
      SerializableType.string,
      List(SerializableType.string, SerializableType.double))
    val result = op(op.features, features)(op.model, m).result
    val classification = result.classification.rdd.collect.toMap
    assert(classification.size == typedTestDataForClassification.vertexNumber)
    assert(classification == typedTestDataForClassification.label)
    val probability = result.probability.rdd.collect.toMap
    assertRoughlyEquals(probability, typedTestDataForClassification.probability, 0.1)
  }
}
