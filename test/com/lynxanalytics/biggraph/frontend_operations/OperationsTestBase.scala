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
  val sql = new SQLController(this, ops)

  case class TestBox(
      operationID: String,
      parameters: Map[String, String],
      inputs: Seq[TestBox]) {

    private def projectRec(boxes: scala.collection.mutable.ListBuffer[Box]): String = {
      val inputNames = inputs.map(
        input => input.projectRec(boxes)
      )
      val name = s"${operationID} ${boxes.length}"
      val inputIds = ops.getBoxMetadata(operationID).inputs.map(_.id)
      assert(inputIds.size == inputNames.size)
      val inputBoxOutputs = inputIds.zip(inputNames).map {
        case (inputID, boxID) => inputID -> BoxOutput(boxID, "project")
      }.toMap

      val box = Box(
        name,
        operationID,
        parameters, 0, 0,
        inputBoxOutputs
      )
      boxes += box
      name
    }

    def project(): RootProjectEditor = {
      val boxes = scala.collection.mutable.ListBuffer[Box]()
      projectRec(boxes)
      val lastBox = boxes.last
      val ws = Workspace(boxes = boxes.toList)
      ws.state(serving.User.fake, ops, lastBox.output("project")).project
    }

    def box(operationID: String,
            parameters: Map[String, String] = Map()): TestBox = {
      TestBox(operationID, parameters, Seq(this))
    }
  }

  def box(operationID: String,
          parameters: Map[String, String] = Map(),
          inputs: Seq[TestBox] = Seq()): TestBox = {
    TestBox(operationID, parameters, inputs)
  }

  def importCSV(files: String): String = {
    ???
    /*
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
    */
  }

}
