package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.controllers._

trait OperationsTestBase extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  def createProject(name: String) = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = name, privacy = "public-write")
    controller.createProject(null, request)
    Project(SymbolPath(name))
  }
  val project = createProject("Test_Project")

  def run(op: String, params: Map[String, String] = Map(), on: Project = project) =
    ops.apply(
      serving.User.fake,
      ProjectOperationRequest(on.projectName, FEOperationSpec(Operation.titleToID(op), params)))

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }
}
