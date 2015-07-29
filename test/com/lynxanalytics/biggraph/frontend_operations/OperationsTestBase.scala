package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.controllers._

trait RunTarget {
  def run(op: String, params: Map[String, String]): Unit
}

trait OperationsTestBase extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  def createProject(name: String): ProjectFrame = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = name, privacy = "public-write")
    controller.createProject(null, request)
    ProjectFrame.fromName(name)
  }
  val projectFrame = createProject("Test_Project")
  def project = projectFrame.viewer.editor

  implicit class SubProjectTarget(p: ProjectFrame) extends RunTarget {
    def run(op: String, params: Map[String, String]): Unit = {
      ops.apply(
        serving.User.fake,
        SubProject(p, Seq()),
        FEOperationSpec(Operation.titleToID(op), params))
    }
  }

  implicit class EditorTarget(ed: ProjectEditor) extends RunTarget {
    def run(opId: String, params: Map[String, String]): Unit = {
      val context = Operation.Context(serving.User.fake, ed.viewer)
      val op = ops.appliedOp(context, FEOperationSpec(Operation.titleToID(opId), params))
      // !!! we need to go to root
      ed.state = op.project.state
    }
  }

  def run(op: String, params: Map[String, String] = Map(), on: RunTarget = projectFrame) = {
    on.run(op, params)
  }

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }
}
