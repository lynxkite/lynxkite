package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class CSVExportTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("We can export a simple graph") {
    val graphManager = cleanGraphManager("csvexport")
    val dataManager = cleanDataManager("csvexport")
    val myGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val myData = dataManager.obtainData(myGraph)
    assert(CSVExport.exportVertices(myData).toString ==
             """|"vertexId","name","age"
                |0,"Adam",20.3
                |1,"Eve",18.2
                |2,"Bob",50.3
                |""".stripMargin)
    assert(CSVExport.exportEdges(myData).toString ==
             """|"srcVertexId","dstVertexId","comment"
                |0,1,"Adam loves Eve"
                |1,0,"Eve loves Adam"
                |2,0,"Bob envies Adam"
                |2,1,"Bob loves Eve"
                |""".stripMargin)
  }

  test("We can save a simple graph to files") {
    val graphManager = cleanGraphManager("csvsave")
    val dataManager = cleanDataManager("csvsave")
    val myGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val myData = dataManager.obtainData(myGraph)
    val targetDir = tempDir("csv_save_target_dir")
    CSVExport.exportToDirectory(myData, targetDir.toString)

    val dirSnapshot = TestUtils.runShellCommand(
        """|cd %s
           |for file in `find . -type f | grep -v crc`; do
           |  echo $file
           |  echo '========'
           |  cat $file
           |  echo '********'
           |done""".stripMargin.format(targetDir.toString))
    assert(dirSnapshot ==
             """|./edge-data/part-00000
                |========
                |0,1,"Adam loves Eve"
                |1,0,"Eve loves Adam"
                |2,0,"Bob envies Adam"
                |2,1,"Bob loves Eve"
                |********
                |./edge-data/_SUCCESS
                |========
                |********
                |./edge-header
                |========
                |"srcVertexId","dstVertexId","comment"
                |********
                |./vertex-data/part-00000
                |========
                |0,"Adam",20.3
                |1,"Eve",18.2
                |2,"Bob",50.3
                |********
                |./vertex-data/_SUCCESS
                |========
                |********
                |./vertex-header
                |========
                |"vertexId","name","age"
                |********
                |""".stripMargin)
  }
}
