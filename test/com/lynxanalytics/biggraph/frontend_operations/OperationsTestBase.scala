package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.controllers._

trait OperationsTestBase extends FunSuite with TestGraphOp with BeforeAndAfterEach {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  val sql = new SQLController(this)

  def clone(original: RootProjectEditor): RootProjectEditor = {
    val res = original.viewer.editor
    res.checkpoint = original.checkpoint
    res
  }

  val user = serving.User.fake
  var ws = Workspace(boxes = List())
  var lastOutput: Option[BoxOutput] = None
  def project = ws.state(user, ops, lastOutput.get).project

  def importCSV(files: String): String = {
    val f = sql.importCSV(serving.User.fake, CSVImportRequest(
      table = s"test-$Timestamp",
      privacy = "private",
      files = files,
      columnNames = Nil,
      delimiter = ",",
      mode = "FAILFAST",
      infer = false,
      overwrite = false,
      columnsToImport = List(),
      limit = None))
    f.id
  }

  def run(opID: String, params: Map[String, String] = Map(), on: ProjectEditor = null): Unit = {
    val box = Box(
      s"box${ws.boxes.size}", opID, params, 0, 0,
      lastOutput.map(o => Map("project" -> o)).getOrElse(Map()))
    ws = ws.copy(boxes = ws.boxes :+ box)
    lastOutput = Some(box.output("project"))
  }

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }

  override def beforeEach() = {
    ws = Workspace.empty
    lastOutput = None
  }
}
