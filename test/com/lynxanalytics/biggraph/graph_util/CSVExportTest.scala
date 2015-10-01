package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

class CSVExportTest extends FunSuite with TestGraphOp {

  test("We can export attributes") {
    val sampleOut = ExampleGraph()().result
    val ids = IdAsAttribute.run(sampleOut.vertices)
    assert(CSVExport.exportVertexAttributes(
      sampleOut.vertices,
      Map(
        "vertexId" -> ids,
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
      Seq("id" -> ids, "id" -> ids, "comment" -> sampleOut.comment)).toString ==
      """|"src_id","dst_id","comment"
         |0,1,"Adam loves Eve"
         |1,0,"Eve loves Adam"
         |2,0,"Bob envies Adam"
         |2,1,"Bob loves Eve"
         |""".stripMargin)
    val names = sampleOut.vertexAttributes("name")
    val ages = sampleOut.vertexAttributes("age")
    assert(CSVExport.exportEdgeAttributes(
      sampleOut.edges,
      Seq("name" -> names, "age" -> ages, "comment" -> sampleOut.comment)).toString ==
      """|"src_name","dst_age","comment"
        |Adam,18.2,"Adam loves Eve"
        |Eve,20.3,"Eve loves Adam"
        |Bob,20.3,"Bob envies Adam"
        |Bob,18.2,"Bob loves Eve"
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
    val sandboxPrefix = TestUtils.getDummyPrefixName(targetDir.toString)
    csvData.saveToDir(HadoopFile(sandboxPrefix))

    val dirSnapshot = TestUtils.runShellCommand(
      """|cd %s
         |for file in `find . -type f | LC_COLLATE=C sort | grep -v crc`; do
         |  echo $file
         |  echo '========'
         |  cat $file | LC_COLLATE=C sort
         |  echo '********'
         |done""".stripMargin.format(targetDir.toString))
    assert(dirSnapshot ==
      """|./data/_SUCCESS
         |========
         |********
         |./data/part-r-00000
         |========
         |18.2,"Eve",1
         |2.0,"Isolated Joe",3
         |20.3,"Adam",0
         |50.3,"Bob",2
         |********
         |./header
         |========
         |"age","name","vertexId"
         |********
         |""".stripMargin)
  }

  test("Export Vector") {
    val g = ExampleGraph()().result
    val neighbors = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[String])
      op(op.connection, g.edges)(op.attr, g.name).result.attr
    }
    val neighborAges = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[Double])
      op(op.connection, g.edges)(op.attr, g.age).result.attr
    }
    assert(CSVExport.exportVertexAttributes(
      g.vertices,
      Map(
        "name" -> g.name,
        "neighbors" -> neighbors,
        "neighborAges" -> neighborAges)).toString ==
      """|"name","neighborAges","neighbors"
         |"Adam",18.2;50.3,"Eve";"Bob"
         |"Eve",20.3;50.3,"Adam";"Bob"
         |"Bob",,
         |"Isolated Joe",,
         |""".stripMargin)
  }
}

