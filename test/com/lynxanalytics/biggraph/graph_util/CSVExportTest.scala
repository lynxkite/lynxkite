package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class CSVExportTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("We can export a simple graph") {
    val graphManager = cleanGraphManager("csvexport")
    val dataManager = cleanDataManager("csvexport")
    val myGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val myData = dataManager.obtainData(myGraph)
    assert(CSVExport.exportVertices(myData).toString ==
             "\"vertexId\",\"name\",\"id\"\n" +
             "0,\"Adam\",0\n" +
             "1,\"Eve\",1\n" +
             "2,\"Bob\",2\n")
    assert(CSVExport.exportEdges(myData).toString ==
             "\"srcVertexId\",\"dstVertexId\",\"comment\"\n" +
             "0,1,\"Adam loves Eve\"\n" +
             "1,0,\"Eve loves Adam\"\n" +
             "2,0,\"Bob envies Adam\"\n" +
             "2,1,\"Bob loves Eve\"\n")
  }
}
