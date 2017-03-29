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
  val user = serving.User.fake

  case class TestBox(
      operationID: String,
      parameters: Map[String, String],
      inputs: Seq[TestBox]) {

    private def projectRec(boxes: scala.collection.mutable.ListBuffer[Box]): String = {
      val inputNames = inputs.map(
        input => input.projectRec(boxes)
      )
      val name = s"${operationID} ${boxes.length}"
      val inputIds = meta.inputs.map(_.id)
      assert(inputIds.size == inputNames.size)
      val inputBoxOutputs = inputIds.zip(inputNames).zip(inputs).map {
        case ((inputId, inputName), inputBox) =>
          val outputs = inputBox.meta.outputs
          assert(outputs.size == 1, s"$inputName has ${outputs.size} outputs.")
          inputId -> BoxOutput(inputName, outputs.head.id)
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

    lazy val (workspace, realBox) = {
      val boxes = scala.collection.mutable.ListBuffer[Box]()
      projectRec(boxes)
      val lastBox = boxes.last
      val ws = Workspace(boxes = boxes.toList)
      (ws, lastBox)
    }

    def meta = ops.getBoxMetadata(operationID)

    def project(): RootProjectEditor = {
      workspace.state(user, ops, realBox.output("project")).project
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

  def importBox(operationID: String,
                parameters: Map[String, String] = Map()): TestBox = {
    val b = box(operationID, parameters)
    val guidFuture = sql.importBox(user, b.realBox)
    val guid = concurrent.Await.result(guidFuture, concurrent.duration.Duration.Inf)
    box(operationID, parameters + ("imported_table" -> guid))
  }
}
