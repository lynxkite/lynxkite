package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_api.BuiltIns

trait OperationsTestBase extends FunSuite with TestGraphOp {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  PrefixRepository.registerPrefix("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  val sql = new SQLController(this, ops)
  val user = serving.User.singleuser
  BuiltIns.createBuiltIns(metaGraphManager)

  case class TestBox(
      operationId: String,
      parameters: Map[String, String],
      parametricParameters: Map[String, String],
      inputMap: Map[String, TestBox]) {

    private def projectRec(boxes: scala.collection.mutable.ListBuffer[Box]): String = {
      val inputBoxNames = inputMap.mapValues(
        box => (box.projectRec(boxes), box)).view.force
      val name = s"${operationId} ${boxes.length}"
      val inputBoxOutputs = inputBoxNames.map {
        case (inputId, (inputName, inputBox)) =>
          val outputs = inputBox.meta.outputs
          assert(outputs.size == 1, s"for $inputName outputs.")
          inputId -> BoxOutput(inputName, outputs.head)
      }.toMap

      val box = Box(
        name,
        operationId,
        parameters, 0, 0,
        inputBoxOutputs,
        parametricParameters)
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
      ctx.allStates(realBox.output("graph")).project

    lazy val exportResult: Scalar[String] =
      ctx.allStates(realBox.output("exported")).exportResult

    lazy val table: Table =
      ctx.allStates(realBox.output("table")).table

    def output(name: String): BoxOutputState =
      ctx.allStates(realBox.output(name))

    def snapshotOutput(name: String, output: String): SnapshotFrame = {
      val state = ctx.allStates(realBox.output(output))
      new DirectoryEntry(SymbolPath.parse(name)).asNewSnapshotFrame(state)
    }

    def changeParameterSettings(changedParameters: Map[String, String]): TestBox = {
      val newParameters = parameters ++ changedParameters
      TestBox(operationId, newParameters, parametricParameters, inputMap)
    }

    def box(
      operationId: String,
      parameters: Map[String, String] = Map(),
      otherInputs: Seq[TestBox] = Seq(),
      parametricParameters: Map[String, String] = Map()): TestBox = {
      val meta = ops.getBoxMetadata(operationId)
      TestBox(
        operationId, parameters, parametricParameters, meta.inputs.zip(this +: otherInputs).toMap)
    }
  }

  def box(
    operationId: String,
    parameters: Map[String, String] = Map(),
    inputs: Seq[TestBox] = Seq(),
    parametricParameters: Map[String, String] = Map(),
    inputMap: Map[String, TestBox] = Map()): TestBox = {
    assert(inputs.isEmpty || inputMap.isEmpty)
    TestBox(
      operationId, parameters, parametricParameters,
      if (inputs.isEmpty) inputMap else {
        val meta = ops.getBoxMetadata(operationId)
        meta.inputs.zip(inputs).toMap
      })
  }

  def importBox(
    operationId: String,
    parameters: Map[String, String] = Map(),
    parametricParameters: Map[String, String] = Map()): TestBox = {
    val b = box(operationId, parameters)
    val guidFuture = sql.importBox(user, b.realBox, Map())
    val response = concurrent.Await.result(guidFuture, concurrent.duration.Duration.Inf)
    val guid = response.guid
    val settings = response.parameterSettings
    box(operationId, parameters + ("imported_table" -> guid) + ("last_settings" -> settings))
  }

  def importCSV(filename: String, options: Map[String, String] = Map()): TestBox =
    importBox("Import CSV", options + ("filename" -> ("OPERATIONSTEST$/" + filename)))

  def importSeq[T <: Product: reflect.runtime.universe.TypeTag](
    columns: Seq[String], rows: Seq[T]): TestBox = {
    val sql = sparkDomain.newSQLContext
    val df = sql.createDataFrame(rows).toDF(columns: _*)
    val table = graph_operations.ImportDataFrame.run(df)
    // Abuse CSV import to load arbitrary table.
    box("Import CSV", Map("imported_table" -> table.gUID.toString))
  }
}
