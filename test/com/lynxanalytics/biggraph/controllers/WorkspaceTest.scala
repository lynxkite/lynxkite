package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import play.api.libs.json
import com.lynxanalytics.biggraph._

class WorkspaceTest extends FunSuite with graph_api.TestGraphOp {
  val controller = new WorkspaceController(this)
  val bigGraphController = new BigGraphController(this)
  val ops = new frontend_operations.Operations(this)
  val user = serving.User.fake
  def context(ws: Workspace, params: (String, String)*) = ws.context(user, ops, params.toMap)

  def create(name: String) =
    controller.createWorkspace(user, CreateWorkspaceRequest(name, "private"))
  def get(name: String): GetWorkspaceResponse =
    controller.getWorkspace(user, GetWorkspaceRequest(name))
  def set(name: String, workspace: Workspace): Unit =
    controller.setWorkspace(user, SetWorkspaceRequest(name, workspace))
  def discard(name: String) =
    bigGraphController.discardEntry(user, DiscardEntryRequest(name))
  def using[T](name: String)(f: => T): T = {
    create(name)
    try {
      f
    } finally discard(name)
  }
  def getOpMeta(ws: String, box: String) =
    controller.getOperationMeta(user, GetOperationMetaRequest(ws, box))

  def getOutputIDs(ws: String) = {
    val allIds = get(ws).outputs
    allIds.map {
      case BoxOutputInfo(bo, id, _, _) => (bo, id)
    }.toMap
  }
  def getProjectOutput(id: String) =
    controller.getProjectOutput(user, GetProjectOutputRequest(id, ""))

  import WorkspaceJsonFormatters._
  import CheckpointRepository._
  def print[T: json.Writes](t: T): Unit = {
    println(json.Json.prettyPrint(json.Json.toJson(t)))
  }

  val pagerankParams = Map(
    "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
    "iterations" -> "5", "direction" -> "all edges")

  test("pagerank on example graph") {
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val pr = Box("pr", "Compute PageRank", pagerankParams, 0, 20, Map("project" -> eg.output("project")))
    val ws = Workspace.from(eg, pr)
    val project = context(ws).allStates(pr.output("project")).project
    import graph_api.Scripting._
    assert(project.vertexAttributes("pagerank").rdd.values.collect.toSet == Set(
      1.4099834026132592, 1.4099834026132592, 0.9892062327983842, 0.19082696197509774))
  }

  test("deltas still work") {
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val merge = Box(
      "merge", "Merge vertices by attribute", Map("key" -> "gender"), 0, 20,
      Map("project" -> eg.output("project")))
    val ws = Workspace.from(eg, merge)
    val project = context(ws).allStates(merge.output("project")).project
    import graph_api.Scripting._
    assert(project.scalars("!vertex_count_delta").value == -2)
  }

  test("validation") {
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val ex = intercept[AssertionError] {
      Workspace.from(eg, eg)
    }
    assert(ex.getMessage.contains("Duplicate box name: eg"))
  }

  test("errors") {
    val pr1 = Box("pr1", "Compute PageRank", pagerankParams, 0, 20, Map())
    val pr2 = pr1.copy(id = "pr2", inputs = Map("project" -> pr1.output("project")))
    val ws = Workspace.from(pr1, pr2)
    val allStates = context(ws).allStates
    val p1 = allStates(pr1.output("project"))
    val p2 = allStates(pr2.output("project"))
    val ex1 = intercept[AssertionError] { p1.project }
    val ex2 = intercept[AssertionError] { p2.project }
    assert(ex1.getMessage.contains("Input project is not connected."))
    assert(ex2.getMessage.contains("Input project has an error."))
  }

  test("getProjectOutput") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val ws = Workspace.from(eg)
      set("test-workspace", ws)
      val id = getOutputIDs("test-workspace")(eg.output("project"))
      val o = getProjectOutput(id)
      val income = o.vertexAttributes.find(_.title == "income").get
      assert(income.metadata("icon") == "money_bag")
    }
  }

  test("getOperationMeta") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val cc = Box(
        "cc", "Find connected components", Map("name" -> "cc", "directions" -> "ignore directions"),
        0, 20, Map())
      val pr = Box("pr", "Compute PageRank", pagerankParams, 0, 20, Map())
      set("test-workspace", Workspace.from(eg, cc, pr))
      intercept[AssertionError] {
        getOpMeta("test-workspace", "pr")
      }
      set(
        "test-workspace",
        Workspace.from(
          eg,
          cc.copy(inputs = Map("project" -> eg.output("project"))),
          pr.copy(inputs = Map("project" -> cc.output("project")))))
      val op = getOpMeta("test-workspace", "pr")
      assert(
        op.parameters.map(_.id) ==
          Seq("apply_to_project", "name", "weights", "iterations", "damping", "direction"))
      assert(
        op.parameters.find(_.id == "weights").get.options.map(_.id) == Seq("!no weight", "weight"))
      assert(
        op.parameters.find(_.id == "apply_to_project").get.options.map(_.id) == Seq("", "|cc"))
    }
  }

  test("2-input operation") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val blanks = Box("blanks", "Create vertices", Map("size" -> "2"), 0, 0, Map())
      val convert = Box(
        "convert", "Convert vertex attribute to double",
        Map("attr" -> "ordinal"), 0, 0, Map("project" -> blanks.output("project")))
      val srcs = Box(
        "srcs", "Derive vertex attribute",
        Map("output" -> "src", "type" -> "string", "expr" -> "ordinal == 0 ? 'Adam' : 'Eve'"),
        0, 0, Map("project" -> convert.output("project")))
      val dsts = Box(
        "dsts", "Derive vertex attribute",
        Map("output" -> "dst", "type" -> "string", "expr" -> "ordinal == 0 ? 'Eve' : 'Bob'"),
        0, 0, Map("project" -> srcs.output("project")))
      val combine = Box(
        "combine", "Import edges for existing vertices",
        Map("attr" -> "name", "src" -> "src", "dst" -> "dst"), 0, 0,
        Map("project" -> eg.output("project"), "edges" -> dsts.output("project")))
      val ws = Workspace.from(eg, blanks, convert, srcs, dsts, combine)
      set("test-workspace", ws)
      val op = getOpMeta("test-workspace", "combine")
      assert(
        op.parameters.find(_.id == "attr").get.options.map(_.id) ==
          Seq("!unset", "age", "gender", "id", "income", "location", "name"))
      assert(
        op.parameters.find(_.id == "src").get.options.map(_.id) ==
          Seq("!unset", "dst", "id", "ordinal", "src"))
      val project = context(ws).allStates(combine.output("project")).project
      import graph_api.Scripting._
      import graph_util.Scripting._
      assert(project.edgeBundle.countScalar.value == 2)
    }
  }

  test("progress success") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val cc = Box(
        "cc", "Find connected components", Map("name" -> "cc", "directions" -> "ignore directions"),
        0, 20, Map("project" -> eg.output("project")))
      // use a different pagerankParams to prevent reusing the attribute computed in an earlier
      // test
      val pr = Box("pr", "Compute PageRank", pagerankParams + ("iterations" -> "3"), 0, 20,
        Map("project" -> cc.output("project")))
      val prOutput = pr.output("project")
      val ws = Workspace.from(eg, cc, pr)
      set("test-workspace", ws)
      val stateIDs = getOutputIDs("test-workspace")
      val prStateID = stateIDs(prOutput)
      val progressBeforePR = controller.getProgress(user,
        GetProgressRequest(List(prStateID))
      ).progress(prStateID).get
      assert(progressBeforePR.inProgress == 0)
      assert(progressBeforePR.computed + progressBeforePR.inProgress
        + progressBeforePR.notYetStarted + progressBeforePR.failed > 0)
      val computedBeforePR = progressBeforePR.computed
      import graph_api.Scripting._
      // trigger PR computation
      context(ws).allStates(pr.output("project")).project.vertexAttributes(pagerankParams("name"))
        .rdd.values.collect
      val progressAfterPR = controller.getProgress(user,
        GetProgressRequest(List(prStateID))
      ).progress(prStateID).get
      val computedAfterPR = progressAfterPR.computed
      assert(computedAfterPR > computedBeforePR)
    }
  }

  test("progress fails") {
    using("test-workspace") {
      // box with unconnected input
      val pr = Box("pr", "Compute PageRank", pagerankParams, 0, 20, Map())
      val prOutput = pr.output("project")
      val ws = Workspace.from(pr)
      set("test-workspace", ws)
      val stateIDs = getOutputIDs("test-workspace")
      val prStateID = stateIDs(prOutput)
      val progress = controller.getProgress(user,
        GetProgressRequest(List(prStateID))
      ).progress(prStateID)
      assert(progress.isEmpty)
    }
  }

  test("circular dependencies") {
    using("test-workspace") {
      val pr1 = Box("pr1", "Compute PageRank", pagerankParams, 0, 20,
        Map("project" -> BoxOutput("pr2", "project")))
      val pr2 = Box("pr2", "Compute PageRank", pagerankParams, 0, 20,
        Map("project" -> BoxOutput("pr1", "project")))
      val pr3 = Box("pr3", "Compute PageRank", pagerankParams, 0, 20,
        Map("project" -> BoxOutput("pr2", "project")))
      val badBoxes = List(pr1, pr2, pr3)
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      set("test-workspace", Workspace.from(eg :: badBoxes: _*))
      val outputInfo = get("test-workspace").outputs
      val outputs = outputInfo.map(BoxOutputInfo.unapply).map(_.get).map {
        case (boxOutput, _, success, _) => (boxOutput, success)
      }.toMap
      for (box <- badBoxes) {
        assert(!outputs(box.output("project")).enabled)
      }
      assert(outputs(eg.output("project")).enabled)
    }
  }

  test("anchor box") {
    using("test-workspace") {
      val anchorBox = Box("anchor", "Anchor", Map(), 0, 0, Map())
      // We have an anchor by default.
      assert(get("test-workspace").workspace.boxes == List(anchorBox))
      // A workspace without an anchor cannot be saved.
      assert(intercept[AssertionError] {
        set("test-workspace", Workspace(List()))
      }.getMessage.contains("Cannot find box: anchor"))
      // We can set its properties though.
      val withDescription = Box("anchor", "Anchor", Map("description" -> "desc"), 10, 0, Map())
      set("test-workspace", Workspace(List(withDescription)))
      assert(get("test-workspace").workspace.boxes == List(withDescription))
      // Duplicate boxes are caught.
      val another = Box("anchor", "Anchor", Map("description" -> "other"), 0, 0, Map())
      assert(intercept[AssertionError] {
        set("test-workspace", Workspace(List(withDescription, another)))
      }.getMessage.contains("Duplicate box name: anchor"))
    }
  }

  test("parametric parameters") {
    val anchor = Box("anchor", "Anchor", Map(
      "parameters" -> json.Json.toJson(List(
        Map("id" -> "p1", "kind" -> "text", "defaultValue" -> "def1"),
        Map("id" -> "p2", "kind" -> "text", "defaultValue" -> "def2"))).toString), 0, 0, Map())
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val const = Box(
      "const", "Add constant vertex attribute",
      Map("value" -> "1", "type" -> "String"), 0, 20, Map("project" -> eg.output("project")),
      Map("name" -> "$p1 $p2"))
    val ws = Workspace(List(anchor, eg, const))
    assert(
      context(ws).allStates(const.output("project")).project
        .vertexAttributes.contains("def1 def2"))
    assert(
      context(ws, "p1" -> "some1").allStates(const.output("project")).project
        .vertexAttributes.contains("some1 def2"))
    assert(intercept[AssertionError] {
      context(ws, "p3" -> "some3").allStates(const.output("project")).project
    }.getMessage.contains("Unrecognized parameter: p3"))
  }
}
