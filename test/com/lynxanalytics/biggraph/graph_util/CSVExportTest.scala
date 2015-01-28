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
      sampleOut.vertices,
      Map(
        "vertexId" -> IdAsAttribute.run(sampleOut.vertices),
        "name" -> sampleOut.name,
        "age" -> sampleOut.age)).toString ==
      """|"age","name","vertexId"
         |20.3,"Adam",0
         |18.2,"Eve",1
         |50.3,"Bob",2
         |2.0,"Isolated Joe",3
         |""".stripMargin)
    assert(CSVExport.exportEdgeAttributes(
      sampleOut.edges,
      Map("comment" -> sampleOut.comment)).toString ==
      """|"srcVertexId","dstVertexId","comment"
         |0,1,"Adam loves Eve"
         |1,0,"Eve loves Adam"
         |2,0,"Bob envies Adam"
         |2,1,"Bob loves Eve"
         |""".stripMargin)
  }

  test("We can save a CSV to a dir") {
    val sampleOut = ExampleGraph()().result
    val csvData = CSVExport.exportVertexAttributes(
      sampleOut.vertices,
      Map(
        "vertexId" -> IdAsAttribute.run(sampleOut.vertices),
        "name" -> sampleOut.name,
        "age" -> sampleOut.age))
    val targetDir = tempDir("csv_save_target_dir")
    csvData.saveToDir(Filename(targetDir.toString))

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
         |20.3,"Adam",0
         |********
         |./data/part-00001
         |========
         |18.2,"Eve",1
         |********
         |./data/part-00002
         |========
         |50.3,"Bob",2
         |********
         |./data/part-00003
         |========
         |2.0,"Isolated Joe",3
         |********
         |./header
         |========
         |"age","name","vertexId"
         |********
         |""".stripMargin)
  }
}

