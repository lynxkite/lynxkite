package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment with BeforeAndAfterEach {
  val controller = new BigGraphController(this)
  val projectName = "Test_Project"
  val projectFrame = ProjectFrame.fromName(projectName)
  val subProject = projectFrame.subproject
  val user = com.lynxanalytics.biggraph.serving.User.fake

  def run(op: String, params: Map[String, String] = Map(), on: String = projectName) =
    controller.projectOp(
      user,
      ProjectOperationRequest(on, FEOperationSpec(Operation.titleToID(op), params)))

  def vattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = subProject.viewer.vertexAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  def eattr[T: TypeTag: ClassTag: Ordering](name: String) = {
    val attr = subProject.viewer.edgeAttributes(name).runtimeSafeCast[T]
    attr.rdd.values.collect.toSeq.sorted
  }

  test("filtering by vertex attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Adam loves Eve", "Eve loves Adam"))
    assert(subProject.toFE.undoOp == "Filter age <40")
  }

  test("filtering by vertex attribute (no edge bundle)") {
    run("Example Graph")
    run("Discard edges")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(subProject.toFE.undoOp == "Filter age <40")
  }

  test("filtering by partially defined vertex attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("income", ">1000")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Bob"))
  }

  test("filtering by edge attribute") {
    run("Example Graph")
    val filter = ProjectAttributeFilter("weight", ">2")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(), List(filter)))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Bob envies Adam", "Bob loves Eve"))
    assert(subProject.toFE.undoOp == "Filter weight >2")
  }

  test("project list") {
    val splash = controller.splash(user, null)
    assert(splash.projects.size == 1)
    assert(splash.projects(0).name == "Test_Project")
    assert(splash.projects(0).vertexCount.isEmpty)
    assert(splash.projects(0).edgeCount.isEmpty)
  }

  test("project list with scalars") {
    run("Example Graph")
    controller.forkProject(user, ForkProjectRequest(from = projectName, to = "new_project"))
    val splash = controller.splash(user, null)
    assert(splash.projects.size == 2)
    assert(splash.projects(1).name == "new_project")
    assert(!splash.projects(1).vertexCount.isEmpty)
    assert(!splash.projects(1).edgeCount.isEmpty)
  }

  test("fork project") {
    run("Example Graph")
    controller.forkProject(user, ForkProjectRequest(from = projectName, to = "forked"))
    val splash = controller.splash(user, null)
    assert(splash.projects.size == 2)
  }

  override def beforeEach() = {
    val path = SymbolPath("projects")
    if (metaGraphManager.tagExists(path)) {
      for (t <- metaGraphManager.lsTag(path)) {
        metaGraphManager.rmTag(t)
      }
    }
    controller.createProject(
      user,
      CreateProjectRequest(name = projectName, notes = "test project", privacy = "private"))
  }
}
