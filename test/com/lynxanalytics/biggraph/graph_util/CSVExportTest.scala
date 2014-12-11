package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.{ ExampleGraph, IdAsAttribute }

class CSVExportTest extends FunSuite with TestGraphOp {
  test("We can export attributes") {
    val sampleOut = ExampleGraph()().result
    assert(CSVExport.exportVertexAttributes(
      Seq(IdAsAttribute.run(sampleOut.vertices), sampleOut.name, sampleOut.age),
      Seq("vertexId", "name", "age")).toSortedString ==
      """|"vertexId","name","age"
         |0,"Adam",20.3
         |1,"Eve",18.2
         |2,"Bob",50.3
         |3,"Isolated Joe",2.0
         |""".stripMargin)
    assert(CSVExport.exportEdgeAttributes(
      sampleOut.edges,
      Seq(sampleOut.comment),
      Seq("comment")).toSortedString ==
      """|"srcVertexId","dstVertexId","comment"
         |0,1,"Adam loves Eve"
         |1,0,"Eve loves Adam"
         |2,0,"Bob envies Adam"
         |2,1,"Bob loves Eve"
         |""".stripMargin)
  }

  test("We can save a CSV to a dir") {
    val sampleOut = ExampleGraph()().result
    val cSVData = CSVExport.exportVertexAttributes(
      Seq(IdAsAttribute.run(sampleOut.vertices), sampleOut.name, sampleOut.age),
      Seq("vertexId", "name", "age"))
    val targetDir = tempDir("csv_save_target_dir")
    cSVData.saveToDir(Filename(targetDir.toString))

    val dirSnapshot = TestUtils.runShellCommand(
      """|cd %s
         |for file in `find . -type f | LC_COLLATE=C sort | grep -v crc`; do
         |  echo $file
         |  echo '========'
         |  cat $file | sort
         |  echo '********'
         |done""".stripMargin.format(targetDir.toString))
    assert(dirSnapshot ==
      """|./data/_SUCCESS
         |========
         |********
         |./data/part-00000
         |========
         |0,"Adam",20.3
         |********
         |./data/part-00001
         |========
         |1,"Eve",18.2
         |********
         |./data/part-00002
         |========
         |2,"Bob",50.3
         |********
         |./data/part-00003
         |========
         |3,"Isolated Joe",2.0
         |********
         |./header
         |========
         |"vertexId","name","age"
         |********
         |""".stripMargin)
  }
}

