package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.collection.mutable

import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.graph_api._

case class SmallGraph(edgeLists: Map[Int, Seq[Int]]) extends MetaGraphOperation {
  def signature = newSignature.outputGraph('vs, 'es)
  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    outputs.putVertexSet('vs, sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ()))))
    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    outputs.putEdgeBundle('es, sc.parallelize(nodePairs.zipWithIndex.map {
      case ((a, b), i) => i.toLong -> Edge(a, b)
    }))
  }
}

object TestWizard extends TestSparkContext {
  def rc = RuntimeContext(sparkContext, numAvailableCores = 1, availableCacheMemoryGB = 100.0)
  def run(op: MetaGraphOperation, inputs: DataSet): DataSet = {
    val outputs = new DataSetBuilder(MetaGraphOperationInstance(op, inputs.metaDataSet))
    op.execute(inputs, outputs, rc)
    return outputs.toDataSet
  }
}

class FindMaxCliquesTest extends FunSuite {
  test("triangle") {
    val sg = TestWizard.run(SmallGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))), DataSet())
    val fmc = TestWizard.run(FindMaxCliques(3), DataSet(
      vertexSets = Map('vsIn -> sg.vertexSets('vs)),
      edgeBundles = Map('esIn -> sg.edgeBundles('es))))
    val vsOut = fmc.vertexSets('vsOut)
    assert(vsOut.rdd.count == 1)
  }
}
