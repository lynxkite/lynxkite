package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._

class ImportGraphTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("import testgraph as csv from separate vertex, edge, vertexheader and edgeheader files") {
    val dir = "/graph_operations/ImportGraphTest/testgraph/"
    val graphManager = cleanGraphManager("importgraph")
    val dataManager = cleanDataManager("importgraph")
    val vertexCSVs = Seq(Filename(getClass.getResource(dir + "vertex-data/part-00000").getFile))
    val edgeCSVs = Seq(Filename(getClass.getResource(dir + "edge-data/part-00000").getFile))
    val vertexHeader = Filename(getClass.getResource(dir + "vertex-header").getFile)
    val edgeHeader = Filename(getClass.getResource(dir + "edge-header").getFile)
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
    assert(TestUtils.RDDToSortedString(graphData.vertices).matches(
      """|\(([0-9]+),0,Adam,20.3\)
         |\(([0-9]+),1,Eve,18.2\)
         |\(([0-9]+),2,Bob,50.3\)""".stripMargin))
    assert(TestUtils.RDDToSortedString(graphData.edges).matches(
      """|Edge\(([0-9]+),([0-9]+),0,1,Adam loves Eve\)
         |Edge\(([0-9]+),([0-9]+),1,0,Eve loves Adam\)
         |Edge\(([0-9]+),([0-9]+),2,0,Bob envies Adam\)
         |Edge\(([0-9]+),([0-9]+),2,1,Bob loves Eve\)""".stripMargin))
  }
  test("import graph from csv as two edge files including header") {
    // different separator, no quotes around strings, newline at eof
    val dir = "/graph_operations/ImportGraphTest/two-edge-csv/"
    val graphManager = cleanGraphManager("importgraph")
    val dataManager = cleanDataManager("importgraph")
    val edgeCSVs = Seq(
      Filename(getClass.getResource(dir + "edges1.csv").getFile),
      Filename(getClass.getResource(dir + "edges2.csv").getFile))
    val edgeHeader = Filename(getClass.getResource(dir + "edges1.csv").getFile)
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
