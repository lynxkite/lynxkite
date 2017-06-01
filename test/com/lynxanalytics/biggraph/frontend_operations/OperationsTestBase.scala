package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving

trait OperationsTestBase extends FunSuite with TestGraphOp {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  val sql = new SQLController(this, ops)
  val user = serving.User.fake

  case class TestBox(
      operationId: String,
      parameters: Map[String, String],
      parametricParameters: Map[String, String],
      inputs: Seq[TestBox]) {

    private def projectRec(boxes: scala.collection.mutable.ListBuffer[Box]): String = {
      val inputNames = inputs.map(
        input => input.projectRec(boxes)
      )
      val name = s"${operationId} ${boxes.length}"
      val inputIds = meta.inputs
      assert(inputNames.size == inputIds.size, s"for $name")
      val inputBoxOutputs = inputIds.zip(inputNames).zip(inputs).map {
        case ((inputId, inputName), inputBox) =>
          val outputs = inputBox.meta.outputs
          assert(outputs.size == 1, s"for $inputName outputs.")
          inputId -> BoxOutput(inputName, outputs.head)
      }.toMap

      val box = Box(
        name,
        operationId,
        parameters, 0, 0,
        inputBoxOutputs,
        parametricParameters
      )
      boxes += box
      name
    }

    lazy val (workspace, realBox) = {
      val boxes = scala.collection.mutable.ListBuffer[Box]()
      projectRec(boxes)
      val lastBox = boxes.last
      val ws = Workspace.from(boxes: _*)
      (ws, lastBox)
    }

    def meta = ops.getBoxMetadata(operationId)

    def ctx = workspace.context(user, ops, Map())

    lazy val project: RootProjectEditor =
      ctx.allStates(realBox.output("project")).project

    lazy val exportResult: Scalar[String] =
      ctx.allStates(realBox.output("exportResult")).exportResult

    def box(operationId: String,
            parameters: Map[String, String] = Map(),
            otherInputs: Seq[TestBox] = Seq(),
            parametricParameters: Map[String, String] = Map()): TestBox = {
      TestBox(operationId, parameters, parametricParameters, this +: otherInputs)
    }
  }

  def box(operationId: String,
          parameters: Map[String, String] = Map(),
          inputs: Seq[TestBox] = Seq(),
          parametricParameters: Map[String, String] = Map()): TestBox = {
    TestBox(operationId, parameters, parametricParameters, inputs)
  }

  def importBox(operationId: String,
                parameters: Map[String, String] = Map()): TestBox = {
    val b = box(operationId, parameters)
    val guidFuture = sql.importBox(user, b.realBox)
    val guid = concurrent.Await.result(guidFuture, concurrent.duration.Duration.Inf)
    box(operationId, parameters + ("imported_table" -> guid))
  }

  def importCSV(filename: String, options: Map[String, String] = Map()): TestBox =
    importBox("Import CSV", options + ("filename" -> ("OPERATIONSTEST$/" + filename)))

  def importSeq[T <: Product: reflect.runtime.universe.TypeTag](
    columns: Seq[String], rows: Seq[T]): TestBox = {
    val sql = dataManager.newSQLContext
    val df = sql.createDataFrame(rows).toDF(columns: _*)
    val table = graph_operations.ImportDataFrame.run(df)
    // Abuse CSV import to load arbitrary table.
    box("Import CSV", Map("imported_table" -> table.gUID.toString))
  }
}
