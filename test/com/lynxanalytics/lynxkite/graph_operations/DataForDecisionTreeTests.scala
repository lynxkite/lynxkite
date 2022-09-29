package com.lynxanalytics.lynxkite.graph_operations

object DataForDecisionTreeTests {
  // I love making long walks if it's not raining and the temperature is
  // pleasant. I take only a short walk if it's not raining, but the weather
  // is too hot or too cold. I hate rain, so I just stay at home if it's raining.
  // Sometimes I'm in a really good mood and go on a long walk in spite of
  // the cold weather.
  case class GraphData(
      labelName: String,
      label: Map[Int, Double],
      featureNames: List[String],
      attrs: Seq[Map[Int, Double]],
      probability: Map[Int, Double] = Map(),
      vertexNumber: Int)
  case class TypedGraphData(
      labelName: String,
      label: Map[Int, String],
      featureNames: List[String],
      stringAttrs: Seq[Map[Int, String]],
      doubleAttrs: Seq[Map[Int, Double]],
      probability: Map[Int, Double] = Map(),
      vertexNumber: Int)

  val trainingData = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 2, 4 -> 1, 5 -> 0, 6 -> 1, 7 -> 2),
    featureNames = List("temperature", "rain"),
    attrs = Seq(
      Map(0 -> -15, 1 -> 20, 2 -> -10, 3 -> 20, 4 -> 35, 5 -> 40, 6 -> -15, 7 -> -15),
      Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 1, 6 -> 0, 7 -> 0)),
    vertexNumber = 8,
  )
  val typedTrainingData = TypedGraphData(
    labelName = "destination",
    label = Map(0 -> "y", 1 -> "x", 2 -> "x", 3 -> "z", 4 -> "y", 5 -> "x", 6 -> "y", 7 -> "z"),
    featureNames = List("temperature", "rain"),
    stringAttrs =
      Seq(Map(0 -> "low", 1 -> "medium", 2 -> "low", 3 -> "medium", 4 -> "high", 5 -> "high", 6 -> "low", 7 -> "low")),
    doubleAttrs = Seq(Map(0 -> 0.0, 1 -> 1.0, 2 -> 1.0, 3 -> 0.0, 4 -> 0.0, 5 -> 1.0, 6 -> 0.0, 7 -> 0.0)),
    vertexNumber = 8,
  )
  val testDataForClassification = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 2, 1 -> 0, 2 -> 1, 3 -> 1, 4 -> 0, 5 -> 0),
    featureNames = List("temperature", "rain"),
    attrs = Seq(
      Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0)),
    probability = Map(0 -> 1, 1 -> 1, 2 -> 1, 3 -> 0.6667, 4 -> 1, 5 -> 1),
    vertexNumber = 6,
  )
  val typedTestDataForClassification = TypedGraphData(
    labelName = "destination",
    label = Map(0 -> "z", 1 -> "x", 2 -> "y", 3 -> "y", 4 -> "x", 5 -> "x"),
    featureNames = List("temperature", "rain"),
    stringAttrs = Seq(Map(0 -> "medium", 1 -> "high", 2 -> "high", 3 -> "low", 4 -> "low", 5 -> "medium")),
    doubleAttrs = Seq(Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0)),
    probability = Map(0 -> 1, 1 -> 1, 2 -> 0.75, 3 -> 0.75, 4 -> 1, 5 -> 1),
    vertexNumber = 6,
  )
  val testDataForRegression = GraphData(
    labelName = "length of the walk",
    label = Map(0 -> 2, 1 -> 0, 2 -> 1, 3 -> 1.3333, 4 -> 0, 5 -> 0),
    featureNames = List("temperature", "rain"),
    attrs = Seq(
      Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0)),
    vertexNumber = 6,
  )
}
