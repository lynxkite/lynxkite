package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment with BeforeAndAfterEach {
  val controller = new BigGraphController(this)
  val project = Project("Test_Project")
  val user = com.lynxanalytics.biggraph.serving.User.fake

  def run(op: String, params: Map[String, String] = Map(), on: Project = project) =
    controller.projectOp(
      user,
      ProjectOperationRequest(on.projectName, FEOperationSpec(Operation.titleToID(op), params)))

  def vattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = project.vertexAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  def eattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = project.edgeAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  test("filtering by vertex attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(project.projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Adam loves Eve", "Eve loves Adam"))
    assert(project.toFE.undoOp == "Filter age <40")
  }

  test("filtering by vertex attribute (no edge bundle)") {
    run("Example Graph")
    run("Discard edges")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(project.projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(project.toFE.undoOp == "Filter age <40")
  }

  test("filtering by partially defined vertex attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("income", ">1000")
    controller.filterProject(user, ProjectFilterRequest(project.projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Bob"))
  }

  test("filtering by edge attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("weight", ">2")
    controller.filterProject(user, ProjectFilterRequest(project.projectName, List(), List(filter)))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Bob envies Adam", "Bob loves Eve"))
    assert(project.toFE.undoOp == "Filter weight >2")
  }

  test("project list") {
    val splash = controller.splash(user, null)
    assert(splash.projects.size == 1)
    assert(splash.projects(0).name == "Test_Project")
  }

  test("fork project") {
    run("Example Graph")
    controller.forkProject(user, ForkProjectRequest(from = project.projectName, to = "forked"))
    val splash = controller.splash(user, null)
    assert(splash.projects.size == 2)
  }

  override def beforeEach() = {
    val path = SymbolPath.fromString("projects")
    if (metaGraphManager.tagExists(path)) {
      for (t <- metaGraphManager.lsTag(path)) {
        metaGraphManager.rmTag(t)
      }
    }
    controller.createProject(
      user,
      CreateProjectRequest(name = project.projectName, notes = "test project", privacy = "private"))
  }
}
