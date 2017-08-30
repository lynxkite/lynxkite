package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests._

class TrainDecisionTreeClassifierTest extends ModelTestBase {
  test("train a decision tree classifier") {
    val m = model(
      method = "Decision tree classification",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber))

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeClassificationModel of depth 3 with 7 nodes
  If (rain <= 0.0)
   If (temperature <= 20.0)
    If (temperature <= -15.0)
     Predict: 1.0
    Else (temperature > -15.0)
     Predict: 2.0
   Else (temperature > 20.0)
    Predict: 1.0
  Else (rain > 0.0)
   Predict: 0.0

Accuracy: 0.875
Support: [0.375, 0.375, 0.25]""")
    assert(m.value.toSQL(sparkContext) == """CASE
 WHEN rain <= 0.0 THEN
  CASE
   WHEN temperature <= 20.0 THEN
    CASE
     WHEN temperature <= -15.0 THEN
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
      val g = graph(trainingDataString.vertexNumber)
      val l = AddVertexAttribute.run(g.vs, trainingDataString.label)
      val features = trainingDataString.attrs.map(attr => AddVertexAttribute.run(g.vs, attr))
      val op = TrainTypedDecisionTreeClassifier(
        trainingDataString.labelName,
        SerializableType.string,
        trainingDataString.featureNames,
        List(SerializableType.string, SerializableType.string),
        impurity = "gini",
        maxBins = 32,
        maxDepth = 5,
        minInfoGain = 0,
        minInstancesPerNode = 1,
        seed = 1234567)
      op(op.features, features)(op.label, l).result.model
    }

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeClassificationModel of depth 3 with 7 nodes
  If (rain in {0.0})
   If (temperature in {0.0,1.0})
    If (temperature in {0.0})
     Predict: 1.0
    Else (temperature not in {0.0})
     Predict: 1.0
   Else (temperature not in {0.0,1.0})
    Predict: 2.0
  Else (rain not in {0.0})
   Predict: 0.0

Accuracy: 0.875
Support: [0.375, 0.375, 0.25]""")
    assert(m.value.toSQL(sparkContext) == """CASE
 WHEN rain IN ('0') THEN
  CASE
   WHEN temperature IN ('high', 'low') THEN
    CASE
     WHEN temperature IN ('high') THEN
      'y'
     ELSE
      'y'
    END
   ELSE
    'z'
  END
 ELSE
  'x'
END AS destination""")
  }
}
