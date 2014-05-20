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
             """|"vertexId","name","id"
                |0,"Adam",0
                |1,"Eve",1
                |2,"Bob",2
                |""".stripMargin)
    assert(CSVExport.exportEdges(myData).toString ==
             """|"srcVertexId","dstVertexId","comment"
                |0,1,"Adam loves Eve"
                |1,0,"Eve loves Adam"
                |2,0,"Bob envies Adam"
                |2,1,"Bob loves Eve"
                |""".stripMargin)
  }
}
