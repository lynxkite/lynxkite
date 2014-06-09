package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api._

case class SmallGraph(edgeLists: Map[Int, Seq[Int]]) extends MetaGraphOperation {
  override def outputs = Name.vertexSet("vs") ++ Name.edgeBundle("es", "vs" -> "vs")
  def execute(inst: MetaGraphOperationInstance, manager: DataManager): DataSet = {
    val sc = manager.runtimeContext.sparkContext
    val vertices = sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ())))
    val edges = sc.parallelize(edgeLists.toSeq.flatMap { case (i, es) => es.map(e => (0l, Edge(i, e))) })
    return DataSet(inst).vertexSet("vs", vertices).edgeBundle("es", edges)
  }
  val gUID = null
}

class TestManager(rc: RuntimeContext) extends DataManager {
  val vertexSets = mutable.Map[VertexSet, VertexSetData]()
  val edgeBundles = mutable.Map[EdgeBundle, EdgeBundleData]()
  def get(vertexSet: VertexSet): VertexSetData = vertexSets(vertexSet)
  def get(edgeBundle: EdgeBundle): EdgeBundleData = edgeBundles(edgeBundle)
  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T] = ???
  def get[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T] = ???
  def saveDataToDisk(component: MetaGraphEntity) = ???
  def runtimeContext: RuntimeContext = rc
}

class FindMaxCliquesTest extends FunSuite {
  test("triangle") {
    val sg = SmallGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))
    val sgi = MetaGraphOperationInstance(sg, MetaDataSet())
    val manager = new TestManager(RuntimeContext(new SparkContext("local", "test"), 1, 100.0))
    val input = sg.execute(sgi, manager)
    val inputVS = input.vertexSets("vs")
    val inputEB = input.edgeBundles("es")
    manager.vertexSets(inputVS.vertexSet) = inputVS
    manager.edgeBundles(inputEB.edgeBundle) = inputEB
    val fmc = FindMaxCliques(3)
    val fmci = MetaGraphOperationInstance(fmc,
      MetaDataSet(Map("input-vs" -> inputVS.vertexSet), Map("input-es" -> inputEB.edgeBundle)))
    val output = fmc.execute(fmci, manager)
    val outputVS = output.vertexSets("output-vs")
    assert(outputVS.rdd.count == 1)
  }
}
