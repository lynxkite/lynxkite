package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class CSVExportTest extends FunSuite with TestGraphOperation {
  test("We can export attributes") {
    val sampleOut = helper.apply(CreateExampleGraphOperation())
    assert(CSVExport.exportVertexAttributes(
      Seq(sampleOut.vertexAttributes('name), sampleOut.vertexAttributes('age)),
      Seq("name", "age"),
      helper.dataManager).toString ==
      """|"vertexId","name","age"
         |0,"Adam",20.3
         |1,"Eve",18.2
         |2,"Bob",50.3
         |""".stripMargin)
    assert(CSVExport.exportEdgeAttributes(
      Seq(sampleOut.edgeAttributes('comment)),
      Seq("comment"),
      helper.dataManager).toString ==
      """|"edgeId","srcVertexId","dstVertexId","comment"
         |0,0,1,"Adam loves Eve"
         |1,1,0,"Eve loves Adam"
         |3,2,1,"Bob loves Eve"
         |2,2,0,"Bob envies Adam"
         |""".stripMargin)
  }

  test("We can save a CSV to a dir") {
    val sampleOut = helper.apply(CreateExampleGraphOperation())
    val cSVData = CSVExport.exportVertexAttributes(
      Seq(sampleOut.vertexAttributes('name), sampleOut.vertexAttributes('age)),
      Seq("name", "age"),
      helper.dataManager)
    val targetDir = tempDir("csv_save_target_dir")
    cSVData.saveToDir(Filename(targetDir.toString))

    val dirSnapshot = TestUtils.runShellCommand(
      """|cd %s
         |for file in `find . -type f | LC_COLLATE=C sort | grep -v crc`; do
         |  echo $file
         |  echo '========'
         |  cat $file
         |  echo '********'
         |done""".stripMargin.format(targetDir.toString))
    assert(dirSnapshot ==
      """|./data/_SUCCESS
         |========
         |********
         |./data/part-00000
         |========
         |0,"Adam",20.3
         |1,"Eve",18.2
         |2,"Bob",50.3
         |********
         |./header
         |========
         |"vertexId","name","age"
         |********
         |""".stripMargin)
  }
}

