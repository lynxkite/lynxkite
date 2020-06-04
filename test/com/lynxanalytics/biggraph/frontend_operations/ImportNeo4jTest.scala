package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.{ ProjectEditor, RootProjectEditor }
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_util
import org.scalatest._
import org.neo4j.harness.{ ServerControls, TestServerBuilders }

class ImportNeo4jTest extends OperationsTestBase with BeforeAndAfterAll {
  graph_util.PrefixRepository.registerPrefix(
    "IMPORTGRAPHTEST$",
    getClass.getResource("/graph_operations/ImportGraphTest").toString)

  val FIXTURE: String =
    """
      UNWIND range(1,5) as pid
      CREATE (p:Person {id:pid, name:"p-"+pid, alive: pid < 3, height: pid*1.5})
      WITH collect(p) as people
      UNWIND people as p1
      WITH p1, people[(id(p1) + 1) % size(people)] as p2
      CREATE (p1)-[:KNOWS {spy: p1.id < 3}]->(p2)
    """

  private var server: ServerControls = _

  override protected def beforeAll(): Unit = {
    server = TestServerBuilders.newInProcessBuilder
      .withConfig("dbms.security.auth_enabled", "false")
      .withConfig("dbms.connector.bolt.listen_address", "localhost:7687")
      .withFixture(FIXTURE)
      .newServer
  }

  override protected def afterAll(): Unit = server.close()

  def vattr[T: reflect.runtime.universe.TypeTag: Ordering](project: ProjectEditor, name: String): Seq[T] =
    project.vertexAttributes(name).runtimeSafeCast[T].rdd.collect.toMap.values.toSeq.sorted

  def importNeo4j(params: Map[String, String]): RootProjectEditor = {
    val box = importBox("Import Neo4j", params)
    box.box("Use table as vertices").project
  }

  def checkNodeImportNeo4j(project: RootProjectEditor) = {
    // Results are collected sorted (see vattr function)
    assert(vattr[String](project, "id") == Seq("1", "2", "3", "4", "5"))
    assert(vattr[String](project, "name") == Seq("p-1", "p-2", "p-3", "p-4", "p-5"))
    assert(vattr[String](project, "alive") == Seq("false", "false", "false", "true", "true"))
    assert(vattr[String](project, "height") == Seq("1.5", "3.0", "4.5", "6.0", "7.5"))
  }

  def checkRelImportNeo4j(project: RootProjectEditor) = {
    // Results are collected sorted (see vattr function)
    assert(vattr[String](project, "source_id$") == Seq("0", "1", "2", "3", "4"))
    assert(vattr[String](project, "target_id$") == Seq("0", "1", "2", "3", "4"))
    assert(vattr[String](project, "spy") == Seq("false", "false", "false", "true", "true"))
  }

  test("import node from Neo4j") {
    checkNodeImportNeo4j(importNeo4j(Map("node_label" -> "Person")))
  }

  test("import node with partitioning from Neo4j") {
    checkNodeImportNeo4j(importNeo4j(Map(
      "node_label" -> "Person",
      "num_partitions" -> "10")))
  }

  test("import node with limit from Neo4j") {
    checkNodeImportNeo4j(importNeo4j(Map(
      "node_label" -> "Person",
      "limit" -> "5")))
  }

  test("import node with selected columns from Neo4j") {
    checkNodeImportNeo4j(importNeo4j(Map(
      "node_label" -> "Person",
      "imported_columns" -> "id,name,alive,height")))
  }

  test("import node with sql query from Neo4j") {
    checkNodeImportNeo4j(importNeo4j(Map(
      "node_label" -> "Person",
      "sql" -> "select * from this")))
  }

  test("import node with infer types from Neo4j") {
    val p = importNeo4j(Map(
      "node_label" -> "Person",
      "infer" -> "yes"))
    assert(vattr[Double](p, "id") == Seq(1, 2, 3, 4, 5))
    assert(vattr[String](p, "name") == Seq("p-1", "p-2", "p-3", "p-4", "p-5"))
    assert(vattr[Boolean](p, "alive") == Seq(false, false, false, true, true))
    assert(vattr[Double](p, "height") == Seq(1.5, 3.0, 4.5, 6.0, 7.5))
  }

  test("import relationship from Neo4j") {
    checkRelImportNeo4j(importNeo4j(Map("relationship_type" -> "KNOWS")))
  }

  test("import relationship with partitioning from Neo4j") {
    checkRelImportNeo4j(importNeo4j(Map(
      "relationship_type" -> "KNOWS",
      "num_partitions" -> "10")))
  }

  test("import relationship with limit from Neo4j") {
    checkRelImportNeo4j(importNeo4j(Map(
      "relationship_type" -> "KNOWS",
      "limit" -> "5")))
  }

  test("import relationship with selected columns from Neo4j") {
    checkRelImportNeo4j(importNeo4j(Map(
      "relationship_type" -> "KNOWS",
      "imported_columns" -> "source_id$, source_id, target_id$, spy")))
  }

  test("import relationship with sql query from Neo4j") {
    checkRelImportNeo4j(importNeo4j(Map(
      "relationship_type" -> "KNOWS",
      "sql" -> "select * from this")))
  }

  test("import relationship with infer types from Neo4j") {
    val p = importNeo4j(Map(
      "relationship_type" -> "KNOWS",
      "infer" -> "yes"))
    assert(vattr[Double](p, "source_id$") == Seq(0, 1, 2, 3, 4))
    assert(vattr[Double](p, "target_id$") == Seq(0, 1, 2, 3, 4))
    assert(vattr[Boolean](p, "spy") == Seq(false, false, false, true, true))
  }

}
