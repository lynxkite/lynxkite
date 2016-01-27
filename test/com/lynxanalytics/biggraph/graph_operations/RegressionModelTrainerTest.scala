package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.model._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class RegressionModelTrainerTest extends FunSuite with TestGraphOp {
  def model(
    method: String,
    labelName: String,
    label: Map[Int, Double],
    featureNames: List[String],
    attrs: Seq[Map[Int, Double]]): Model = {
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq())).result
    val l = AddDoubleVertexAttribute.run(g.vs, label)
    val a = attrs.map(attr => AddDoubleVertexAttribute.run(g.vs, attr))
    val op = RegressionModelTrainer(method, labelName, featureNames)
    op(op.features, a)(op.label, l).result.model.value
  }

  test("test model parameters") {
    val m = model(
      method = "Linear regression",
      labelName = "age",
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      featureNames = List("yob"),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)))
    val impl = m.load(sparkContext)
    assert(impl.isInstanceOf[LinearRegressionModelImpl])
  }
}
