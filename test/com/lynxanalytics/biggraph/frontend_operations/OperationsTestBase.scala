package com.lynxanalytics.biggraph.frontend_operations

import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.controllers._

import scala.collection.mutable.ListBuffer

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

  class BoxChain() {
    var chain = new ListBuffer[Box]()

    def lastOutput: Option[BoxOutput] = {
      chain.toList.lastOption.map {
        box => box.output("project")
      }
    }
    def add(box: Box): Unit = {
      chain += box
    }
    def asList: List[Box] = {
      chain.toList
    }
  }

  case class TestBox(
      operationID: String,
      parameters: Map[String, String],
      inputs: Seq[TestBox]) {

    private def projectRec(boxes: scala.collection.mutable.ListBuffer[Box]): String = {
      val inputNames = inputs.map(
        input => input.projectRec(boxes)
      )
      val name = s"box${boxes.length}"
      val inputIds = ops.getBoxMetadata(operationID).inputs.map(_.id)
      assert(inputIds.size == inputNames.size)
      val inputBoxOutputs = inputIds.zip(inputNames).map(
        idName => idName._1 -> BoxOutput(idName._2, "project")
      ).toMap

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
      println("Result:")
      boxes.foreach(println)
      ws.state(user, ops, lastBox.output("project")).project
    }

    def enforceComputation = project()

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

  var chain: BoxChain = null

  def project(): RootProjectEditor = {
    println("run boxes:")
    ws.boxes.foreach(println)
    ws.state(user, ops, chain.lastOutput.get).project
  }

  def enforceComputation = project

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

  def run(opID: String,
          params: Map[String, String] = Map(), on: BoxChain = null): BoxChain = {
    val inputIds = ops.getBoxMetadata(opID).inputs.map(_.id)
    val boxOutputs: Map[String, BoxOutput] = inputIds.size match {
      case 0 =>
        chain = new BoxChain()
        Map()
      case 1 =>
        assert(on == null, s"BoxChain $on will be unused")
        Map(inputIds(0) -> chain.lastOutput.get)
      case 2 =>
        assert(on != null, s"For inputs: $inputIds we need another BoxChain")
        Map(
          inputIds(0) -> on.lastOutput.get,
          inputIds(1) -> chain.lastOutput.get
        )
      case _ => ???
    }

    val box = Box(
      s"box${ws.boxes.size}",
      opID,
      params,
      0, 0,
      boxOutputs)

    ws = ws.copy(boxes = ws.boxes :+ box)
    chain.add(box)
    chain
  }

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }

  override def beforeEach() = {
    ws = Workspace.empty
    chain = null
  }
}
