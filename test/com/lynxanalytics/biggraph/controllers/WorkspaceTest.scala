package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api.BuiltIns
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class WorkspaceTest extends FunSuite with graph_api.TestGraphOp {
  val controller = new WorkspaceController(this)
  val bigGraphController = new BigGraphController(this)
  val ops = new frontend_operations.Operations(this)
  BuiltIns.createBuiltIns(metaGraphManager)
  val user = serving.User.singleuser
  def context(ws: Workspace, params: (String, String)*) = ws.context(user, ops, params.toMap)

  def create(name: String) =
    controller.createWorkspace(user, CreateWorkspaceRequest(name))
  def get(name: String): GetWorkspaceResponse =
    controller.getWorkspace(user, WorkspaceReference(name))
  def set(name: String, workspace: Workspace): Unit =
    controller.setWorkspace(user, SetWorkspaceRequest(WorkspaceReference(name), workspace))
  def discard(name: String) =
    bigGraphController.discardEntry(user, DiscardEntryRequest(name))
  def using[T](name: String)(f: => T): T = {
    create(name)
    try {
      f
    } finally discard(name)
  }
  def getOpMeta(ws: String, box: String) =
    controller.getOperationMeta(user, GetOperationMetaRequest(WorkspaceReference(ws), box))

  def getOutputIds(ws: String) = {
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
    assert(dataManager.get(project.scalars("!vertex_count_delta")) == -2)
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
    val pr3 = pr1.copy(id = "pr3", inputs = Map("project" -> pr2.output("project")))
    val sql = Box(
      "sql", "SQL2", Map(), 0, 100,
      Map("one" -> pr2.output("project"), "two" -> pr3.output("project")))
    val ws = Workspace.from(pr1, pr2, pr3, sql)
    val allStates = context(ws).allStates
    val p1 = allStates(pr1.output("project"))
    val p2 = allStates(sql.output("table"))
    val ex1 = intercept[AssertionError] { p1.project }
    val ex2 = intercept[AssertionError] { p2.project }
    assert(ex1.getMessage == "Input project of box pr1 is not connected.")
    assert(ex2.getMessage == """Inputs one, two of box sql have errors:
  one: Input project of box pr2 has an error:
    project: Input project of box pr1 is not connected.
  two: Input project of box pr3 has an error:
    project: Input project of box pr2 has an error:
      project: Input project of box pr1 is not connected.""")
  }

  test("long errors") {
    val input1 = Box("input1", "Input", Map(), 0, 20, Map())
    val copy1 = Box("copy1", "Copy scalar from other project", Map(), 0, 50, Map(
      "project" -> input1.output("input"), "scalar" -> input1.output("input")))
    val copy2 = copy1.copy(id = "copy2", inputs = Map(
      "project" -> copy1.output("project"), "scalar" -> copy1.output("project")))
    val copy3 = copy2.copy(id = "copy3", inputs = Map(
      "project" -> copy2.output("project"), "scalar" -> copy2.output("project")))
    val ws = Workspace.from(input1, copy1, copy2, copy3)
    val allStates = context(ws).allStates
    val p = allStates(copy3.output("project"))
    val ex = intercept[AssertionError] { p.project }
    assert(ex.getMessage == """Inputs project, scalar of box copy3 have errors:
  project: Inputs project, scalar of box copy2 have errors:
    project: Inputs project, scalar of box copy1 have errors:
      project: Unconnected
      scalar: Unconnected
    scalar: Inputs project, scalar of box copy1 have errors:
      project: Unconnected
      scalar: Unconnected
...
      project: Unconnected
      scalar: Unconnected""")
  }

  test("getProjectOutput") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val ws = Workspace.from(eg)
      set("test-workspace", ws)
      val id = getOutputIds("test-workspace")(eg.output("project"))
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
        op.parameters.find(_.id == "apply_to_project").get.options.map(_.id) == Seq("", ".cc"))
    }
  }

  test("ID and Long are serialized and deserialized") {
    val start = Box("start", "Create vertices", Map("size" -> "10"), 0, 0, Map())
    val deriveFromId = Box(
      "id", "Derive vertex attribute",
      Map("output" -> "out1", "expr" -> "id.toString"),
      0, 0, Map("project" -> start.output("project")))
    val deriveFromLong = Box(
      "long", "Derive vertex attribute",
      Map("output" -> "out2", "expr" -> "ordinal.toString"),
      0, 0, Map("project" -> deriveFromId.output("project")))
    val ws = Workspace.from(start, deriveFromId, deriveFromLong)
    context(ws).allStates(deriveFromLong.output("project")).project
  }

  test("2-input operation") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val blanks = Box("blanks", "Create vertices", Map("size" -> "2"), 0, 0, Map())
      val convert = Box(
        "convert", "Convert vertex attribute to Double",
        Map("attr" -> "ordinal"), 0, 0, Map("project" -> blanks.output("project")))
      val srcs = Box(
        "srcs", "Derive vertex attribute",
        Map("output" -> "src", "expr" -> "if (ordinal == 0) \"Adam\" else \"Eve\""),
        0, 0, Map("project" -> convert.output("project")))
      val dsts = Box(
        "dsts", "Derive vertex attribute",
        Map("output" -> "dst", "expr" -> "if (ordinal == 0) \"Eve\" else \"Bob\""),
        0, 0, Map("project" -> srcs.output("project")))
      val combine = Box(
        "combine", "Use table as edges",
        Map("attr" -> "name", "src" -> "src", "dst" -> "dst"), 0, 0,
        Map("project" -> eg.output("project"), "table" -> dsts.output("project")))
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
      val stateIds = getOutputIds("test-workspace")
      val prStateId = stateIds(prOutput)
      val progressBeforePR = controller.getProgress(
        user,
        List(prStateId))(prStateId).get
      assert(progressBeforePR.inProgress == 0)
      assert(progressBeforePR.computed + progressBeforePR.inProgress
        + progressBeforePR.notYetStarted + progressBeforePR.failed > 0)
      val computedBeforePR = progressBeforePR.computed
      import graph_api.Scripting._
      // trigger PR computation
      context(ws).allStates(pr.output("project")).project.vertexAttributes(pagerankParams("name"))
        .rdd.values.collect
      val progressAfterPR = controller.getProgress(
        user,
        List(prStateId))(prStateId).get
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
      val stateIds = getOutputIds("test-workspace")
      val prStateId = stateIds(prOutput)
      val progress = controller.getProgress(
        user,
        List(prStateId))(prStateId)
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

  test("non-circular dependencies (#5971)") {
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val imp = Box(
      "imp", "Use table as segmentation", Map(
        "name" -> "self", "base_id_attr" -> "name",
        "base_id_column" -> "name", "seg_id_column" -> "name"), 0, 0,
      Map("project" -> eg.output("project"), "table" -> eg.output("project")))
    val ws = Workspace.from(eg, imp)
    val p = context(ws).allStates(imp.output("project")).project
    assert(p.segmentationNames.contains("self"))
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

  def anchorWithParams(params: (String, String, String)*): Box = {
    Box("anchor", "Anchor", Map(
      "parameters" -> json.Json.toJson(params.toList.map {
        case (id, kind, defaultValue) =>
          Map("id" -> id, "kind" -> kind, "defaultValue" -> defaultValue)
      }).toString), 0, 0, Map())
  }

  test("parametric parameters") {
    val anchor = anchorWithParams(("p1", "text", "def1"), ("p2", "text", "def2"))
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

  test("parametric parameters - access attributes") {
    val anchorBox = Box("anchor", "Anchor", Map(), 0, 0, Map())
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val const = Box(
      "const", "Add constant vertex attribute",
      Map("value" -> "1", "type" -> "String"),
      0, 20,
      Map("project" -> eg.output("project")),
      Map("name" ->
        """${vertexAttributes.filter(_.typeName == "Double").map(_.name).mkString("-")}"""))
    val ws = Workspace(List(anchorBox, eg, const))
    assert(
      context(ws).allStates(const.output("project")).project
        .vertexAttributes.contains("age-income"))
  }

  test("parametric parameters - import CSV") {
    val anchor = anchorWithParams(("PREFIX", "text", "IMPORTGRAPHTEST$"))
    val csv = {
      val csv = Box("csv", "Import CSV", Map(), 0, 0, Map(),
        Map("filename" -> ("$PREFIX/testgraph/vertex-header")))
      val ws = Workspace(List(anchor, csv))
      val resourceDir = getClass.getResource("/graph_operations/ImportGraphTest").toString
      println(resourceDir)
      graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", resourceDir)
      create("test-parametric-parameters-import-CSV")
      set("test-parametric-parameters-import-CSV", ws)
      val wsRef = controller.ResolvedWorkspaceReference(
        user, WorkspaceReference("test-parametric-parameters-import-CSV"))
      val workspaceParams = wsRef.ws.workspaceExecutionContextParameters(wsRef.params)
      val ops = new com.lynxanalytics.biggraph.frontend_operations.Operations(this)
      val sql = new SQLController(this, ops)
      val guidFuture = sql.importBox(user, csv, workspaceParams)
      val response = concurrent.Await.result(guidFuture, concurrent.duration.Duration.Inf)
      val guid = response.guid
      val settings = response.parameterSettings
      csv.copy(parameters = csv.parameters + ("imported_table" -> guid) + ("last_settings" -> settings))
    }
    val vs = Box(
      "vs", "Use table as vertices",
      Map(), 0, 20, Map("table" -> csv.output("table")),
      Map())
    val ws = Workspace(List(anchor, csv, vs))
    val attrs = context(ws).allStates(vs.output("project")).project.vertexAttributes
    assert(attrs.contains("vertexId"))
    assert(attrs.contains("name"))
    assert(attrs.contains("age"))
  }

  test("compute box - project") {
    val anchor = anchorWithParams()
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val seg = Box("seg", "Segment by String attribute", Map(), 0, 0, Map("project" -> eg.output("project")))
    val compute = Box("compute", "Compute inputs", Map(), 0, 0, Map("input" -> seg.output("project")))
    create("test-compute-box-project")
    val ws = Workspace(List(anchor, eg, seg, compute))
    set("test-compute-box-project", ws)
    val op = controller.getOperation(user, GetOperationMetaRequest(WorkspaceReference("test-compute-box-project"), compute.id))
    assert(op.asInstanceOf[TriggerableOperation].getGUIDs("input").size == 22)
  }

  test("compute box - table") {
    val anchor = anchorWithParams()
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val sql = Box("sql", "SQL1", Map(), 0, 0, Map("input" -> eg.output("project")))
    val compute = Box("compute", "Compute inputs", Map(), 0, 0, Map("input" -> sql.output("table")))
    create("test-compute-box-table")
    val ws = Workspace(List(anchor, eg, sql, compute))
    set("test-compute-box-table", ws)
    val op = controller.getOperation(user, GetOperationMetaRequest(WorkspaceReference("test-compute-box-table"), compute.id))
    // ExampleGraph has 6 vertex attributes including ID.
    assert(op.asInstanceOf[TriggerableOperation].getGUIDs("input").size == 1)
  }

  test("save to snapshot") {
    import scala.concurrent._
    import scala.concurrent.duration._
    val anchor = anchorWithParams()
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val sts = Box("sts", "Save to snapshot",
      Map("path" -> "test-save-to-snapshot_snapshot"), 0, 0, Map("state" -> eg.output("project")))
    create("test-save-to-snapshot")
    val ws = Workspace(List(anchor, eg, sts))
    set("test-save-to-snapshot", ws)
    val op = controller.getOperation(user, GetOperationMetaRequest(
      WorkspaceReference("test-save-to-snapshot"), sts.id)).asInstanceOf[TriggerableOperation]
    val gdc = new GraphDrawingController(this) // Triggerable ops need this to compute entity data.
    Await.ready(op.trigger(controller, gdc), Duration.Inf)
    val entry = DirectoryEntry.fromName("test-save-to-snapshot_snapshot")
    assert(entry.exists)
    assert(entry.asInstanceOf[SnapshotFrame].getState.kind == "project")
  }

  test("custom box") {
    using("test-custom-box") {
      val anchor = anchorWithParams(("param1", "text", "def1"))
      val inputBox = Box("input", "Input", Map("name" -> "in1"), 0, 0, Map())
      val pr = Box(
        "pr", "Compute PageRank", pagerankParams - "name", 0, 0,
        Map("project" -> inputBox.output("input")), Map("name" -> "pr_$param1"))
      val outputBox = Box(
        "output", "Output", Map("name" -> "out1"), 0, 0, Map("output" -> pr.output("project")))
      set("test-custom-box", Workspace(List(anchor, inputBox, pr, outputBox)))

      // Now use "test-custom-box" as a custom box.
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val cb = Box("cb", "test-custom-box", Map(), 0, 0, Map("in1" -> eg.output("project")))
      assert({
        // Relying on default parameters.
        val ws = Workspace.from(eg, cb)
        context(ws).allStates(cb.output("out1")).project.vertexAttributes.contains("pr_def1")
      })
      assert({
        // Providing a specific parameter value.
        val ws = Workspace.from(eg, cb.copy(parameters = Map("param1" -> "xyz")))
        context(ws).allStates(cb.output("out1")).project.vertexAttributes.contains("pr_xyz")
      })
    }
  }

  test("custom box with aggregation") {
    using("test-custom-box") {
      val anchor = anchorWithParams()
      val inputBox = Box("input", "Input", Map("name" -> "in1"), 0, 0, Map())
      val aggr = {
        val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
        val aggr = Box("aggr", "Aggregate edge attribute globally",
          Map("prefix" -> "", "aggregate_weight" -> "sum"), 0, 0,
          Map("project" -> eg.output("project")))
        set("test-custom-box", Workspace(List(anchor, eg, aggr)))
        // We connect aggr to the inputBox instead of eg0.
        get("test-custom-box").workspace.boxes.find(_.id == "aggr").get
          .copy(inputs = Map("project" -> inputBox.output("input")))
      }

      // Let's create a custom box.
      val outputBox = Box(
        "output", "Output", Map("name" -> "out1"), 0, 0, Map("output" -> aggr.output("project")))
      set("test-custom-box", Workspace(List(anchor, inputBox, aggr, outputBox)))

      // Now use "test-custom-box" as a custom box.
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val cb1 = Box("cb1", "test-custom-box", Map(), 0, 0, Map("in1" -> eg.output("project")))
      assert({
        val ws = Workspace.from(eg, cb1)
        context(ws).allStates(cb1.output("out1")).project.scalars.contains("weight_sum")
      })

      // Remove the comment edge attribute and see if the custom box still works.
      val dea = Box("d", "Discard edge attributes", Map("name" -> "comment"), 0, 0,
        Map("project" -> eg.output("project")))
      val cb2 = Box("cb2", "test-custom-box", Map(), 0, 0, Map("in1" -> dea.output("project")))
      assert({
        val ws = Workspace.from(eg, dea, cb2)
        context(ws).allStates(cb2.output("out1")).project.scalars.contains("weight_sum")
      })
    }
  }

  test("broken parameters: parametric apply_to_project with error") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val pr = Box(
        "pr", "Compute PageRank", pagerankParams, 0, 100, Map("project" -> eg.output("project")),
        Map("apply_to_project" -> "$x"))
      val ws = Workspace.from(eg, pr)
      val project = context(ws).allStates(pr.output("project"))
      set("test-workspace", ws)
      val op = getOpMeta("test-workspace", "pr")
      // $x is undefined, so the value of "apply_to_project" is unavailable. It would be used for
      // defining the later parameters, so those parameters are missing.
      assert(op.parameters.map(_.id) == Seq("apply_to_project"))
      // The error is reported by marking the operation as disabled.
      // Also the output carries an error.
      assert(op.status.disabledReason.contains("not found: value x"))
      assert(project.isError)
    }
  }

  test("broken parameters: parametric PageRank iterations with error") {
    using("test-workspace") {
      val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
      val pr = Box(
        "pr", "Compute PageRank", Map(
          "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
          "direction" -> "all edges"),
        0, 100, Map("project" -> eg.output("project")),
        Map("iterations" -> "$x"))
      val ws = Workspace.from(eg, pr)
      val project = context(ws).allStates(pr.output("project"))
      set("test-workspace", ws)
      val op = getOpMeta("test-workspace", "pr")
      // $x is undefined, so the value of "iterations" is unavailable. No parameters depend on it,
      // so the parameter list is not affected. Everything is editable.
      assert(op.parameters.map(_.id) ==
        Seq("apply_to_project", "name", "weights", "iterations", "damping", "direction"))
      // The output carries an error.
      assert(project.isError)
    }
  }

  test("workspace reduction") {
    val eg1 = Box("eg1", "Create example graph", Map(), 0, 0, Map())
    val eg2 = Box("eg2", "Create example graph", Map(), 0, 0, Map())
    val pr = Box(
      "pr", "Compute PageRank", Map(
        "name" -> "pagerank", "damping" -> "0.85", "weights" -> "!no weight",
        "direction" -> "all edges"),
      0, 100, Map("project" -> eg1.output("project")),
      Map("iterations" -> "$x"))
    val ws = Workspace.from(eg1, eg2, pr)
    val ctx = context(ws)
    assert(ctx.ws.boxes.size == 4)
    assert(ctx.reduced(pr).ws.boxes.size == 3)
  }

  test("import union of table snapshots") {
    val eg = Box("eg", "Create example graph", Map(), 0, 0, Map())
    val sql = Box("sql", "SQL1", Map(), 0, 0, Map("input" -> eg.output("project")))
    create("import_table_snapshot")
    set("import_table_snapshot", Workspace.from(eg, sql))
    val outputs = getOutputIds("import_table_snapshot")
    val stateId = outputs(sql.output("table"))
    controller.createSnapshot(user, CreateSnapshotRequest("import_table_snapshot_1", stateId))
    controller.createSnapshot(user, CreateSnapshotRequest("import_table_snapshot_2", stateId))
    val is = Box("is", "Import union of table snapshots",
      Map("paths" -> "import_table_snapshot_1,import_table_snapshot_2"), 0, 0, Map())
    val sql2 = Box("sql2", "SQL1",
      Map("sql" -> "select count(1) from input"), 0, 0, Map("input" -> is.output("table")))
    val table = context(Workspace.from(is, sql2)).allStates(sql2.output("table")).table
    import graph_api.Scripting._
    assert(table.df.head().getLong(0) == 8)
  }
}
