package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.graph_api.Edge
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.table.TableImport

class ExportImportOperationTest extends OperationsTestBase {
  test("Imports from implicit tables") {
    val project2 = clone(project)
    run("Create example graph" /* , on = project2 */ )
    run(
      "Find connected components",
      Map(
        "name" -> "cc",
        "directions" -> "ignore directions")
    /*, on = project2 */ )
    run(
      "Convert vertex attribute to string",
      Map("attr" -> "id") /*,
      on = project2.segmentation("cc") */ )
    val project2Checkpoint = s"!checkpoint(${project2.checkpoint.get},ExampleGraph)"

    // Import vertices as vertices
    run(
      "Import vertices",
      Map(
        "table" -> (project2Checkpoint + "|vertices"),
        "id_attr" -> "new_id"))
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
      "Import vertices",
      Map(
        "table" -> (project2Checkpoint + "|edges"),
        "id_attr" -> "id"))
    assert(project.vertexSet.rdd.count == 4)
    assert(project.edgeBundle == null)

    {
      val vAttrs = project.vertexAttributes.toMap
      // 2 original edge attr, 2 * 6 src and dst attributes + 1 id
      assert(vAttrs.size == 15)
      assert(vAttrs("edge_weight").rdd.map(_._2).collect.toSet == Set(1.0, 2.0, 3.0, 4.0))
      assert(
        vAttrs("src_name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Bob", "Bob", "Eve"))
      assert(
        vAttrs("dst_name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Adam", "Eve", "Eve"))
    }
    run("Discard vertices")

    // Import belongs to as edges
    run("Import vertices and edges from a single table",
      Map(
        "table" -> (project2Checkpoint + "|cc|belongs_to"),
        "src" -> "base_name",
        "dst" -> "segment_id"))
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

      val baseName = eAttrs("base_name").runtimeSafeCast[String].rdd
      val segmentId = eAttrs("segment_id").runtimeSafeCast[String].rdd
      val componentMap = baseName.sortedJoin(segmentId).values.collect.toMap
      assert(componentMap.size == 4)
      assert(componentMap("Adam") == componentMap("Eve"))
      assert(componentMap("Adam") == componentMap("Bob"))
      assert(componentMap("Adam") != componentMap("Isolated Joe"))
      assert(
        vAttrs("stringID").rdd.map(_._2).collect.toSet ==
          Set("Adam", "Eve", "Bob", "Isolated Joe", "0", "3"))
    }
  }

  test("Import edges for existing vertices") {
    val rows = Seq(
      ("Adam", "Eve", "value1"),
      ("Isolated Joe", "Bob", "value2"),
      ("Eve", "Alice", "value3"))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("src", "dst", "value")
    val table = TableImport.importDataFrameAsync(dataFrame)
    val tableFrame = DirectoryEntry.fromName("test_edges_table").asNewTableFrame(table, "")
    val tablePath = s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|vertices"
    run("Create example graph")
    run("Import edges for existing vertices", Map(
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

  test("Import vertex attributes") {
    val rows = Seq(
      ("Adam", "value1"),
      ("Isolated Joe", "value2"),
      ("Alice", "value3"))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("row_id", "value")
    val table = TableImport.importDataFrameAsync(dataFrame)
    val tableFrame = DirectoryEntry.fromName("test_attr_table").asNewTableFrame(table, "")
    val tablePath = s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|vertices"
    run("Create example graph")
    run("Import vertex attributes", Map(
      "table" -> tablePath,
      "id_attr" -> "name",
      "id_column" -> "row_id",
      "prefix" -> "imported"
    ))
    val valueAttr = project.vertexAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (3, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Import edge attributes") {
    val rows = Seq(
      ("Adam loves Eve", "value1"),
      ("Bob envies Adam", "value2"),
      ("Squirrell loves Peanuts", "value3"))
    // The last row does not match any edges in the example graph.
    // Therefore we expect it to be discarded.
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("row_id", "value")
    val table = TableImport.importDataFrameAsync(dataFrame)
    val tableFrame = DirectoryEntry.fromName("test_edge_attr_table").asNewTableFrame(table, "")
    val tablePath = s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|vertices"
    run("Create example graph")
    run("Import edge attributes", Map(
      "table" -> tablePath,
      "id_attr" -> "comment",
      "id_column" -> "row_id",
      "prefix" -> "imported"
    ))
    val valueAttr = project.edgeAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (2, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Attributes are not overwritten during import") {
    val rows = Seq(
      ("1", "Bob", "12"),
      ("2", "Eve", "23"),
      ("3", "Chris", "34"))
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("id", "name", "age")
    val table = TableImport.importDataFrameAsync(dataFrame)
    val tableFrame = DirectoryEntry.fromName("test_attr_overwrite_table").asNewTableFrame(table, "")
    val tablePath = s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|vertices"

    val edgeRows = Seq(("Adam loves Eve", "1.0"))
    val edgeDataFrame = sql.createDataFrame(edgeRows).toDF("new_comment", "weight")
    val edgeTable = TableImport.importDataFrameAsync(edgeDataFrame)
    val edgeTableFrame = DirectoryEntry.fromName("test_attr_overwrite_table2").asNewTableFrame(edgeTable, "")
    val edgeTablePath = s"!checkpoint(${edgeTableFrame.checkpoint}, ${edgeTableFrame.name})|vertices"

    run("Create example graph")
    run("Convert vertex attribute to string", Map(
      "attr" -> "id"
    ))
    val ex = intercept[java.lang.AssertionError] {
      run("Import vertex attributes", Map(
        "table" -> tablePath,
        "id_attr" -> "id",
        "id_column" -> "id",
        "prefix" -> ""
      ))
    }
    assert(ex.getMessage.contains(
      "Cannot import column `id`. Attribute already exists."))
    val ex2 = intercept[java.lang.AssertionError] {
      run("Import edge attributes", Map(
        "table" -> edgeTablePath,
        "id_attr" -> "comment",
        "id_column" -> "new_comment",
        "prefix" -> ""
      ))
    }
    assert(ex2.getMessage.contains(
      "Cannot import column `weight`. Attribute already exists."))

  }
}
