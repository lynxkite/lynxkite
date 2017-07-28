package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.{ graph_operations, TestUtils }
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
    val src = graph_operations.VertexToEdgeAttribute.srcAttribute(ids, sampleOut.edges)
    val dst = graph_operations.VertexToEdgeAttribute.dstAttribute(ids, sampleOut.edges)
    assert(CSVExport.exportEdgeAttributes(
      sampleOut.edges,
      Seq("id" -> src, "id" -> dst, "comment" -> sampleOut.comment)).toString ==
      """|"id","id","comment"
                 |0,1,"Adam loves Eve"
                 |1,0,"Eve loves Adam"
                 |2,0,"Bob envies Adam"
                 |2,1,"Bob loves Eve"
                 |""".stripMargin)
    val names = sampleOut.name
    val nameAttr = graph_operations.VertexToEdgeAttribute.srcAttribute(names, sampleOut.edges)
    val ages = sampleOut.age
    val ageAttr = graph_operations.VertexToEdgeAttribute.dstAttribute(ages, sampleOut.edges)
    assert(CSVExport.exportEdgeAttributes(
      sampleOut.edges,
      Seq("name" -> nameAttr, "age" -> ageAttr, "comment" -> sampleOut.comment)).toString ==
      """|"name","age","comment"
                |"Adam",18.2,"Adam loves Eve"
                |"Eve",20.3,"Eve loves Adam"
                |"Bob",20.3,"Bob envies Adam"
                |"Bob",18.2,"Bob loves Eve"
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
         |for file in `find . -type f | LC_ALL=C sort | grep -v crc`; do
         |  echo $file
         |  echo '========'
         |  cat $file | LC_ALL=C sort
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
      op(op.connectionBySrc, HybridEdgeBundle.bySrc(g.edges))(op.attr, g.name).result.attr
    }
    val neighborAges = {
      val op = AggregateByEdgeBundle(Aggregator.AsVector[Double])
      op(op.connectionBySrc, HybridEdgeBundle.bySrc(g.edges))(op.attr, g.age).result.attr
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

