package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.controllers._

trait OperationsTestBase extends FunSuite with TestGraphOp {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  val sql = new SQLController(this)

  def clone(original: RootProjectEditor): RootProjectEditor = {
    val res = original.viewer.editor
    res.checkpoint = original.checkpoint
    res
  }

  val project = new RootProjectEditor(RootProjectState.emptyState)
  project.checkpoint = Some("")

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

  def run(opId: String, params: Map[String, String] = Map(), on: ProjectEditor = project) = {
    /*
    val context = Operation.Context(serving.User.fake, on.viewer)
    val result = ops.applyAndCheckpoint(context, FEOperationSpec(Operation.titleToID(opId), params))
    on.rootEditor.rootState = result
    */
    ???
  }

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }
}
