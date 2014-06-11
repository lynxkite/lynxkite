package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._

class ImportGraphTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("import testgraph as csv from separate vertex, edge, vertexheader and edgeheader files") {
    val dir = "/graph_operations/ImportGraphTest/testgraph/"
    val bigGraphManager = cleanGraphManager("importgraph")
    val graphDataManager = cleanDataManager("importgraph")
    val vertexCSVs = Seq(Filename(getClass.getResource(dir + "vertex-data/part-00000").getFile))
    val edgeCSVs = Seq(Filename(getClass.getResource(dir + "edge-data/part-00000").getFile))
    val vertexHeader = Filename(getClass.getResource(dir + "vertex-header").getFile)
    val edgeHeader = Filename(getClass.getResource(dir + "edge-header").getFile)
    val vertexIdFieldName = "vertexId"
    val sourceEdgeFieldName = "srcVertexId"
    val destEdgeFieldName = "dstVertexId"
    val delimiter = ","
    val skipFirstRow = false
    val importedGraph = bigGraphManager.deriveGraph(Seq(), CSVImport(
      vertexHeader, vertexCSVs,
      edgeHeader, edgeCSVs,
      vertexIdFieldName, sourceEdgeFieldName, destEdgeFieldName,
      delimiter, skipFirstRow))
    val graphData = graphDataManager.obtainData(importedGraph)
    assert(graphData.vertices.count === 3)
    assert(graphData.edges.count === 4)
    assert(TestUtils.RDDToSortedString(graphData.vertices).matches(
      """|\(\d+,0,Adam,20.3\)
         |\(\d+,1,Eve,18.2\)
         |\(\d+,2,Bob,50.3\)""".stripMargin))
    assert(TestUtils.RDDToSortedString(graphData.edges).matches(
      """|Edge\(\d+,\d+,0,1,Adam loves Eve\)
         |Edge\(\d+,\d+,1,0,Eve loves Adam\)
         |Edge\(\d+,\d+,2,0,Bob envies Adam\)
         |Edge\(\d+,\d+,2,1,Bob loves Eve\)""".stripMargin))
  }
  test("import graph from csv as two edge files including header") {
    // different separator, no quotes around strings, newline at eof, files with wildcard
    val dir = "/graph_operations/ImportGraphTest/two-edge-csv/"
    val bigGraphManager = cleanGraphManager("importgraph")
    val graphDataManager = cleanDataManager("importgraph")
    val edgeCSVs = Seq(Filename(getClass.getResource(dir).toString + "*"))
    val edgeHeader = Filename(getClass.getResource(dir + "edges1.csv").getFile)
    val vertexIdAttrName = "vertexId"
    val edgeSourceFieldName = "srcVertexId"
    val edgeDestFieldName = "dstVertexId"
    val delimiter = "|"
    val skipFirstRow = true
    val importedGraph = bigGraphManager.deriveGraph(Seq(), EdgeCSVImport(
      edgeHeader, edgeCSVs,
      vertexIdAttrName, edgeSourceFieldName, edgeDestFieldName,
      delimiter, skipFirstRow, Set()))
    val graphData = graphDataManager.obtainData(importedGraph)
    assert(graphData.vertices.count === 6)
    assert(graphData.edges.count === 8)
  }
  test("import graph from csv with numerical IDs") {
    val dir = "/graph_operations/ImportGraphTest/num-ids/"
    val bigGraphManager = cleanGraphManager("importgraph")
    val graphDataManager = cleanDataManager("importgraph")
    val csv = Filename(getClass.getResource(dir + "edges.csv").getFile)
    val edgeSourceFieldName = "srcVertexId"
    val edgeDestFieldName = "dstVertexId"
    val delimiter = "|"
    val skipFirstRow = true
    val importedGraph = bigGraphManager.deriveGraph(Seq(), EdgeCSVImportNum(
      csv, Seq(csv),
      edgeSourceFieldName, edgeDestFieldName,
      delimiter, skipFirstRow, Set()))
    val graphData = graphDataManager.obtainData(importedGraph)
    assert(graphData.vertices.count === 5)
    assert(graphData.vertices.map(_._1).collect.toSet == Set(100, 200, 300, 400, 500))
    assert(graphData.edges.count === 4)
  }
}
