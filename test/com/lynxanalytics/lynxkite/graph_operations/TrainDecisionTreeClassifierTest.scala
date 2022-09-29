package com.lynxanalytics.lynxkite.graph_operations

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_operations.DataForDecisionTreeTests._

class TrainDecisionTreeClassifierTest extends ModelTestBase {
  test("train a decision tree classifier") {
    val m = model(
      method = "Decision tree classification",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber),
    )

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeClassificationModel: depth=3, numNodes=7, numClasses=3, numFeatures=2
  If (rain <= 0.5)
   If (temperature <= 27.5)
    If (temperature <= -12.5)
     Predict: 1.0
    Else (temperature > -12.5)
     Predict: 2.0
   Else (temperature > 27.5)
    Predict: 1.0
  Else (rain > 0.5)
   Predict: 0.0

Accuracy: 0.875
Support: [0.375, 0.375, 0.25]""")
    assert(m.value.toSQL(sparkContext) == """CASE
 WHEN rain <= 0.5 THEN
  CASE
   WHEN temperature <= 27.5 THEN
    CASE
     WHEN temperature <= -12.5 THEN
      1.0
     ELSE
      2.0
    END
   ELSE
    1.0
  END
 ELSE
  0.0
END AS length of the walk""")
  }

  test("train a decision tree classifier - string values") {
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

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeClassificationModel: depth=2, numNodes=5, numClasses=3, numFeatures=2
  If (rain <= 0.5)
   If (temperature in {0.0,1.0})
    Predict: 1.0
   Else (temperature not in {0.0,1.0})
    Predict: 2.0
  Else (rain > 0.5)
   Predict: 0.0

Accuracy: 0.875
Support: [0.375, 0.375, 0.25]""")
    assert(m.value.toSQL(sparkContext) == """CASE
 WHEN rain <= 0.5 THEN
  CASE
   WHEN temperature IN ('high', 'low') THEN
    'y'
   ELSE
    'z'
  END
 ELSE
  'x'
END AS destination""")
  }
}
