package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTest extends BigGraphControllerTestBase {
  // TODO: Depends on #5874.
  /*
  test("filtering by vertex attribute") {
    run("Create example graph")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Adam loves Eve", "Eve loves Adam"))
    assert(subProject.toFE.undoOp == "Filter age <40")
  }

  test("filtering by defined vertex attribute") {
    run("Create example graph")
    val filter = ProjectAttributeFilter("income", "*")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Bob"))
    assert(eattr[String]("comment") == Seq("Bob envies Adam"))
    assert(subProject.toFE.undoOp == "Filter income *")
  }

  test("filtering by vertex attribute (no edge bundle)") {
    run("Create example graph")
    run("Discard edges")
    val filter = ProjectAttributeFilter("age", "<40")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Adam", "Eve", "Isolated Joe"))
    assert(subProject.toFE.undoOp == "Filter age <40")
  }

  test("filtering by partially defined vertex attribute") {
    run("Create example graph")
    val filter = ProjectAttributeFilter("income", ">1000")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(filter), List()))
    assert(vattr[String]("name") == Seq("Bob"))
  }

  test("filtering by edge attribute") {
    run("Create example graph")
    val filter = ProjectAttributeFilter("weight", ">2")
    controller.filterProject(user, ProjectFilterRequest(projectName, List(), List(filter)))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(eattr[String]("comment") == Seq("Bob envies Adam", "Bob loves Eve"))
    assert(subProject.toFE.undoOp == "Filter weight >2")
  }
  */

  def list(dir: String) = controller.entryList(user, EntryListRequest(dir))
  def search(basePath: String, query: String) =
    controller.entrySearch(user, EntrySearchRequest(basePath, query, includeNotes = false))

  // TODO: Depends on #5860.
  /*
  test("project list with scalars") {
    run("Create example graph")
    controller.forkEntry(user, ForkEntryRequest(from = projectName, to = "new_project"))
    val pl = list("")
    assert(pl.objects.size == 2)
    assert(pl.objects(1).name == "new_project")
    assert(!pl.objects(1).vertexCount.isEmpty)
    assert(!pl.objects(1).edgeCount.isEmpty)
  }

  test("fork project") {
    run("Create example graph")
    controller.forkEntry(user, ForkEntryRequest(from = projectName, to = "forked"))
    assert(list("").objects.size == 2)
  }
  */

  test("create directory") {
    controller.createDirectory(user, CreateDirectoryRequest(
      name = "foo/bar", privacy = "private"))
    assert(list("").directories == Seq("foo"))
    assert(list("foo").objects.isEmpty)
    assert(list("foo").directories == Seq("foo/bar"))
    controller.discardEntry(user, DiscardEntryRequest(name = "foo"))
    assert(list("").directories.isEmpty)
  }

  val wc = new WorkspaceController(this)

  test("list and search workspace") {
    wc.createWorkspace(user, CreateWorkspaceRequest(name = "foo/bar"))
    val bar = Seq(FEEntryListElement("foo/bar", "workspace", "workspace"))
    assert(list("").directories == Seq("foo"))
    assert(list("foo").directories.isEmpty)
    assert(list("foo").objects == bar)
    assert(search("", "ba").objects == bar)
    assert(search("foo", "ar").objects == bar)
  }

  test("list and search snapshot") {
    wc.createWorkspace(user, CreateWorkspaceRequest(name = "foo/bar"))
    val ws = Workspace.from(Box("eg", "Create example graph", Map(), 0, 0, Map()))
    val stateId = wc.setWorkspace(user, SetWorkspaceRequest(WorkspaceReference("foo/bar"), ws))
      .outputs(0).stateId
    wc.createSnapshot(user, CreateSnapshotRequest("foo/snapshot", stateId))

    assert(list("").directories == Seq("foo"))
    assert(list("foo").directories.isEmpty)
    // Snapshots return too much info in FEEntryListElement to assert all here.
    assert(list("foo").objects.map { e => (e.name, e.objectType) } ==
      Seq(("foo/bar", "workspace"), ("foo/snapshot", "snapshot")))
    assert(search("", "snap").objects.map { e => (e.name, e.objectType) } ==
      Seq(("foo/snapshot", "snapshot")))
    assert(search("foo", "shot").objects.map { e => (e.name, e.objectType) } ==
      Seq(("foo/snapshot", "snapshot")))
  }
}
