package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val controller = new BigGraphController(this)
  val project = Project("Test_Project")
  project.notes = "test project"
  project.checkpointAfter("") // Initialize project.

  def run(op: String, params: Map[String, String] = Map(), on: Project = project) =
    controller.projectOp(
      ProjectOperationRequest(on.projectName, FEOperationSpec(op.replace(" ", "-"), params)))

  def vattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = project.vertexAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  def eattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = project.edgeAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  test("filtering") {
    run("Example Graph")
    val filter = FEVertexAttributeFilter(project.vertexAttributes("age").gUID.toString, "<40")
    controller.filterProject(ProjectFilterRequest(project.projectName, List(filter)))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Adam loves Eve", "Eve loves Adam"))
  }
}
