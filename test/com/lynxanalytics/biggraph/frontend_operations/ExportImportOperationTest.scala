package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.graph_api.Edge
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.TestDataManager
import com.lynxanalytics.biggraph.table.TableImport

class ExportImportOperationTest extends OperationsTestBase with TestDataManager {
  test("SQL import & export vertices") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export vertex attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "delete" -> "yes",
      "attrs" -> "id,name,age,income,gender"))
    run("Import vertices from a database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "columns" -> "name,age,income,gender",
      "key" -> "id",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("1000.0", "2000.0"))
  }

  test("SQL import & export edges") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export edge attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph_edges",
      "delete" -> "yes",
      "attrs" -> "weight,comment"))
    run("Import vertices and edges from single database table", Map(
      "db" -> db,
      "table" -> "example_graph_edges",
      "columns" -> "srcVertexId,dstVertexId,weight,comment",
      "key" -> "srcVertexId",
      "src" -> "srcVertexId",
      "dst" -> "dstVertexId"))
    assert(project.vertexSet.rdd.count == 3) // Isolated Joe is lost.
    val weight = project.edgeAttributes("weight").runtimeSafeCast[String]
    val comment = project.edgeAttributes("comment").runtimeSafeCast[String]
    assert(weight.rdd.values.collect.toSeq.sorted == Seq("1.0", "2.0", "3.0", "4.0"))
    assert(comment.rdd.values.collect.toSeq.sorted == Seq("Adam loves Eve", "Bob envies Adam", "Bob loves Eve", "Eve loves Adam"))
  }

  test("CSV import & export vertices") {
    run("Example Graph")
    val path = dataManager.repositoryPath + "/csv-export-test"
    run("Export vertex attributes to file", Map(
      "path" -> path.symbolicName,
      "link" -> "link",
      "attrs" -> "id,name,age,income,gender",
      "format" -> "CSV"))
    val header = (path + "/header").readAsString
    run("Import vertices from CSV files", Map(
      "files" -> (path + "/data/*").symbolicName,
      "header" -> header,
      "delimiter" -> ",",
      "filter" -> "",
      "omitted" -> "",
      "allow_corrupt_lines" -> "no",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("", "", "1000.0", "2000.0"))
  }

  def runImport(allowCorruptLines: Boolean) = {
    val allow =
      if (allowCorruptLines) "yes"
      else "no"
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/bad-lines.csv",
      "header" -> "src,dst,attr",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "allow_corrupt_lines" -> allow,
      "filter" -> ""))

    project.edgeAttributes("attr").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted

  }

  test("Assert ill-formed csv") {
    intercept[org.apache.spark.SparkException] {
      runImport(false)
    }
  }

  test("Allow ill-formed csv") {
    assert(runImport(true) == Seq("good", "good", "good"))
  }

  test("Imports from implicit tables") {
    val project2 = clone(project)
    run("Example Graph", on = project2)
    run(
      "Connected components",
      Map(
        "name" -> "cc",
        "directions" -> "ignore directions"),
      on = project2)
    run(
      "Vertex attribute to string",
      Map("attr" -> "id"),
      on = project2.segmentation("cc"))
    val project2Checkpoint = s"!checkpoint(${project2.checkpoint.get},ExampleGraph)"

    // Import vertices as vertices
    run(
      "Import vertices from table",
      Map(
        "table" -> (project2Checkpoint + "|!vertices"),
        "id-attr" -> "new_id"))
    assert(project.vertexSet.rdd.count == 4)
    assert(project.edgeBundle == null)

    {
      val vAttrs = project.vertexAttributes.toMap
      // 6 original +1 new_id
      assert(vAttrs.size == 7)
      assert(vAttrs("id") == vAttrs("new_id"))
      assert(
        vAttrs("name").rdd.map(_._2).collect.toSet == Set("Adam", "Eve", "Bob", "Isolated Joe"))
    }
    run("Discard vertices")

    // Import edges as vertices
    run(
      "Import vertices from table",
      Map(
        "table" -> (project2Checkpoint + "|!edges"),
        "id-attr" -> "id"))
    assert(project.vertexSet.rdd.count == 4)
    assert(project.edgeBundle == null)

    {
      val vAttrs = project.vertexAttributes.toMap
      // 2 original edge attr, 2 * 6 src and dst attributes + 1 id
      assert(vAttrs.size == 15)
      assert(vAttrs("weight").rdd.map(_._2).collect.toSet == Set(1.0, 2.0, 3.0, 4.0))
      assert(
        vAttrs("src$name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Bob", "Bob", "Eve"))
      assert(
        vAttrs("dst$name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Adam", "Eve", "Eve"))
    }
    run("Discard vertices")

    // Import belongs to as edges
    run("Import vertices and edges from a single table",
      Map(
        "table" -> (project2Checkpoint + "|cc|!belongsTo"),
        "src" -> "base$name",
        "dst" -> "segment$id"))
    // 4 nodes in 2 segments
    assert(project.vertexSet.rdd.count == 6)
    assert(project.edgeBundle.rdd.count == 4)

    {
      val vAttrs = project.vertexAttributes.toMap
      val eAttrs = project.edgeAttributes.toMap
      // id, stringId
      assert(vAttrs.size == 2)
      // 6 from base vertices, 2 from connected components
      assert(eAttrs.size == 8)

      val baseName = eAttrs("base$name").runtimeSafeCast[String].rdd
      val segmentId = eAttrs("segment$id").runtimeSafeCast[String].rdd
      val compnentMap = baseName.sortedJoin(segmentId).values.collect.toMap
      assert(compnentMap.size == 4)
      assert(compnentMap("Adam") == compnentMap("Eve"))
      assert(compnentMap("Adam") == compnentMap("Bob"))
      assert(compnentMap("Adam") != compnentMap("Isolated Joe"))
      assert(
        vAttrs("stringID").rdd.map(_._2).collect.toSet ==
          Set("Adam", "Eve", "Bob", "Isolated Joe", "0", "3"))
    }
    run("Discard vertices")
  }

  test("Import edges for existing vertices from table") {
    val rows = Seq(
      ("Adam", "Eve", "value1"),
      ("Isolated Joe", "Bob", "value2"),
      ("Eve", "Alice", "value3"))
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("src", "dst", "value")
    val table = TableImport.importDataFrame(dataFrame)
    val tableFrame = DirectoryEntry.fromName("test_edges_table").asNewTableFrame(table)
    val tablePath = s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|!vertices"
    run("Example Graph")
    run("Import edges for existing vertices from table", Map(
      "table" -> tablePath,
      "attr" -> "name",
      "src" -> "src",
      "dst" -> "dst"
    ))
    assert(Seq(Edge(0, 1), Edge(3, 2)) ==
      project.edgeBundle.rdd.collect.toSeq.map(_._2))
    val valueAttr = project.edgeBundle.rdd
      .join(project.edgeAttributes("value").runtimeSafeCast[String].rdd)
      .values
    assert(Seq((Edge(0, 1), "value1"), (Edge(3, 2), "value2")) ==
      valueAttr.collect.toSeq.sorted)
  }
}
