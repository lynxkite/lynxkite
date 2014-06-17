package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.collection.mutable

import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.graph_api._

object TestWizard extends TestSparkContext {
  def rc = RuntimeContext(sparkContext, numAvailableCores = 1, availableCacheMemoryGB = 100.0)
  def run(op: MetaGraphOperation, inputs: DataSet): DataSet = {
    val outputs = new DataSetBuilder(MetaGraphOperationInstance(op, inputs.metaDataSet))
    op.execute(inputs, outputs, rc)
    return outputs.toDataSet
  }
}

class FindMaxCliquesTest extends FunSuite with TestGraphOperation {
  test("triangle") {
    val helper = cleanHelper
    val (sgv, sge) = helper.smallGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))
    val fmcOut = helper.apply(
      FindMaxCliques(3),
      Map(
        'vsIn -> sgv,
        'esIn -> sge))
    assert(helper.localData(fmcOut.vertexSets('cliques)).size == 1)
  }
}
