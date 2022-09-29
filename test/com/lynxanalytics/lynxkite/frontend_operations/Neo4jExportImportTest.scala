package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.controllers.DirectoryEntry
import com.lynxanalytics.lynxkite.graph_api.Edge
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class Neo4jContainer
    extends org.testcontainers.containers.Neo4jContainer[Neo4jContainer]("neo4j:4.0.8-enterprise")

class Neo4jExportImportTest extends OperationsTestBase {
  val server = new Neo4jContainer()
    .withoutAuthentication
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")

  def exportExampleGraph() = {
    val res = box("Create example graph")
      .box("Export graph to Neo4j", Map("url" -> server.getBoltUrl)).exportResult
    dataManager.get(res)
  }

  test("full graph export and import") {
    server.start()
    exportExampleGraph()
    val p = importBox("Import from Neo4j", Map("url" -> server.getBoltUrl)).project
    assert(p.vertexAttributes.toMap.keySet == Set(
      "!LynxKite ID",
      "!LynxKite export timestamp",
      "<id>",
      "<labels>",
      "age",
      "gender",
      "id",
      "income",
      "location",
      "name"))
    assert(p.edgeAttributes.toMap.keySet == Set(
      "<rel_id>",
      "<rel_type>",
      "<source_id>",
      "<target_id>",
      "comment",
      "weight"))
    assert(get(p.vertexAttributes("name")).values.toSet == Set("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(get(p.edgeAttributes("weight")).values.toSet == Set(1.0, 2.0, 3.0, 4.0))
    server.stop()
  }

  test("attribute export") {
    server.start()
    exportExampleGraph()
    val g = box("Create example graph").box("Compute PageRank").box("Compute dispersion")
    dataManager.get(g.box(
      "Export vertex attributes to Neo4j",
      Map("url" -> server.getBoltUrl, "keys" -> "name", "to_export" -> "page_rank")).exportResult)
    dataManager.get(g.box(
      "Export edge attributes to Neo4j",
      Map("url" -> server.getBoltUrl, "keys" -> "comment", "to_export" -> "dispersion")).exportResult)
    val p = importBox("Import from Neo4j", Map("url" -> server.getBoltUrl)).project
    assert(get(p.vertexAttributes("page_rank")).values.toSet ==
      get(g.project.vertexAttributes("page_rank")).values.toSet)
    assert(get(p.edgeAttributes("dispersion")).values.toSet ==
      get(g.project.edgeAttributes("dispersion")).values.toSet)
    server.stop()
  }

  test("export with labels and types") {
    server.start()
    val res = box("Create example graph").box(
      "Export graph to Neo4j",
      Map(
        "url" -> server.getBoltUrl,
        "node_labels" -> "gender",
        "relationship_type" -> "comment")).exportResult
    dataManager.get(res)
    val p = importBox("Import from Neo4j", Map("url" -> server.getBoltUrl)).project
    assert(
      get(p.vertexAttributes("<labels>")).values.toList
        .flatMap(_.asInstanceOf[scala.collection.mutable.WrappedArray[String]]).sorted
        == Seq("Female", "Male", "Male", "Male"))
    assert(get(p.edgeAttributes("<rel_type>")).values.toSet
      == Set("Adam loves Eve", "Bob envies Adam", "Bob loves Eve", "Eve loves Adam"))
    server.stop()
  }
}
