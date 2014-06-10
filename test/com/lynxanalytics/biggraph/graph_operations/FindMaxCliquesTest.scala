package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.collection.mutable

import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.graph_api._

case class SmallGraph(edgeLists: Map[Int, Seq[Int]]) extends MetaGraphOperation {
  def signature = newSignature.outputGraph("vs", "es")
  def execute(inputs: DataSet, outputs: DataSet, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    outputs.putVertexSet("vs", sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ()))))
    outputs.putEdgeBundle("es", sc.parallelize(edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => (0l, Edge(i, e)))
    }))
  }
  val gUID = null
}

object TestWizard extends TestSparkContext {
  def rc = RuntimeContext(sparkContext, 1, 100.0)
  def run(op: MetaGraphOperation, inputs: DataSet): DataSet = {
    val outputs = DataSet(defaultInstance = Some(MetaGraphOperationInstance(op, inputs.metaDataSet)))
    op.execute(inputs, outputs, rc)
    return outputs
  }
}

class FindMaxCliquesTest extends FunSuite {
  test("triangle") {
    val sg = TestWizard.run(SmallGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))), DataSet())
    val fmc = TestWizard.run(FindMaxCliques(3), DataSet(
      vertexSets = mutable.Map("vsIn" -> sg.vertexSets("vs")),
      edgeBundles = mutable.Map("esIn" -> sg.edgeBundles("es"))))
    val vsOut = fmc.vertexSets("vsOut")
    assert(vsOut.rdd.count == 1)
  }
}
