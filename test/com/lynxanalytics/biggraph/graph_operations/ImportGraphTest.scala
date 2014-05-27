package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._

class ImportGraphTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("import testgraph as csv from separate vertex, edge, vertexheader and edgeheader files") {
    val graphManager = cleanGraphManager("importgraph")
    val dataManager = cleanDataManager("importgraph")
    val vertexCSVs = Seq(
      Filename(getClass.getResource("/csv_save_target_dir/vertex-data/part-00000")
      .getFile, "", ""))
    val edgeCSVs = Seq(
      Filename(getClass.getResource("/csv_save_target_dir/edge-data/part-00000")
      .getFile, "", ""))
    val vertexHeader = Filename(
      getClass.getResource("/csv_save_target_dir/vertex-header")
      .getFile, "", "")
    val edgeHeader = Filename(
      getClass.getResource("/csv_save_target_dir/edge-header")
      .getFile, "", "")
    val vertexIdFieldName = "vertexId"
    val sourceEdgeFieldName = "srcVertexId"
    val destEdgeFieldName = "dstVertexId"
    val delimiter = ","
    val skipFirstRow = false
    val importedGraph = graphManager.deriveGraph(Seq(), CSVImport(
      vertexHeader, vertexCSVs,
      edgeHeader, edgeCSVs,
      vertexIdFieldName, sourceEdgeFieldName, destEdgeFieldName,
      delimiter, skipFirstRow))
    val graphData = dataManager.obtainData(importedGraph)
    assert(graphData.vertices.count === 3)
    assert(graphData.edges.count === 4)
  }
  test("import graph from csv as two edge files including header") {
    // different separator, no quotes around strings, newline at eof
    val graphManager = cleanGraphManager("importgraph")
    val dataManager = cleanDataManager("importgraph")
    val edgeCSVs = Seq(
      Filename(getClass.getResource("/edgecsvs-w-header/edges1.csv")
      .getFile, "", ""),
      Filename(getClass.getResource("/edgecsvs-w-header/edges2.csv")
      .getFile, "", ""))
    val edgeHeader = Filename(
      getClass.getResource("/edgecsvs-w-header/edges1.csv")
      .getFile, "", "")
    val vertexIdFieldName = "vertexId"
    val sourceEdgeFieldName = "srcVertexId"
    val destEdgeFieldName = "dstVertexId"
    val delimiter = "|"
    val skipFirstRow = true
    val importedGraph = graphManager.deriveGraph(Seq(), EdgeCSVImport(
      edgeHeader, edgeCSVs,
      vertexIdFieldName, sourceEdgeFieldName, destEdgeFieldName,
      delimiter, skipFirstRow))
    val graphData = dataManager.obtainData(importedGraph)
    assert(graphData.vertices.count === 6)
    assert(graphData.edges.count === 8)
  }
}
