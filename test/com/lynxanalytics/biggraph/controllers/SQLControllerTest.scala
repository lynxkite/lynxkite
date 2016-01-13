package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

// TODO(gsvigruha): refactor this and BigGraphControllerTest to get rid of code duplication
class SQLControllerTest extends FunSuite with TestGraphOp with BeforeAndAfterEach {
  val controller = new BigGraphController(this)
  val sqlController = new SQLController(this)
  val projectName = "Test_Project"
  val projectFrame = ProjectFrame.fromName(projectName)
  val subProject = projectFrame.subproject
  val user = com.lynxanalytics.biggraph.serving.User.fake

  def run(op: String, params: Map[String, String] = Map(), on: String = projectName) =
    controller.projectOp(
      user,
      ProjectOperationRequest(on, FEOperationSpec(Operation.titleToID(op), params)))

  test("sql on vertices") {
    run("Example Graph")
    val result = sqlController.runSQLQuery(user, SQLRequest(project = projectName,
      sql = "select name from `!vertices` where age < 40"))
    assert(result.header.sameElements(Array("name")))
    assert(result.data.sameElements(Array(Seq("Adam"), Seq("Eve"), Seq("Isolated Joe"))))
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
