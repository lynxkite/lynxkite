package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ImportGraphTest extends FunSuite with TestGraphOp {
  /*test("import testgraph as csv from separate vertex, edge, vertexheader and edgeheader files") {
    val dir = "/graph_operations/ImportGraphTest/testgraph/"
    val vertexCSVs = Filename(getClass.getResource(dir + "vertex-data/part-00000").getFile)
    val edgeCSVs = Filename(getClass.getResource(dir + "edge-data/part-00000").getFile)
    val vertexHeader = Filename(getClass.getResource(dir + "vertex-header").getFile)
    val edgeHeader = Filename(getClass.getResource(dir + "edge-header").getFile)
    val vertexIdFieldName = "vertexId"
    val sourceEdgeFieldName = "srcVertexId"
    val destEdgeFieldName = "dstVertexId"
    val delimiter = ","
    val vertexData = ImportVertexListWithNumericIDs(
      CSV(vertexCSVs, delimiter, ImportUtil.header(vertexHeader)),
      vertexIdFieldName).result
    val vs = vertexData.vertices
    val edgeData = {
      val op = ImportEdgeListForExistingVertexSet(
        CSV(edgeCSVs, delimiter, ImportUtil.header(edgeHeader)),
        sourceEdgeFieldName, destEdgeFieldName)
      op(op.sources, vs)(op.destinations, vs).result
    }
    assert(TestUtils.RDDToSortedString(vertexData.attrs("name").rdd) ==
      """|(0,Adam)
         |(1,Eve)
         |(2,Bob)""".stripMargin)
    val edges = edgeData.edges.rdd
    val comments = edgeData.attrs("comment").rdd
    assert(TestUtils.RDDToSortedString(edges.join(comments).values) ==
      """|(Edge(0,1),Adam loves Eve)
         |(Edge(1,0),Eve loves Adam)
         |(Edge(2,0),Bob envies Adam)
         |(Edge(2,1),Bob loves Eve)""".stripMargin)
  }*/

  //  test("import graph from csv as two edge files including header") {
  //    // different separator, no quotes around strings, newline at eof, files with wildcard
  //    val dir = "/graph_operations/ImportGraphTest/two-edge-csv/"
  //    val edgeCSVs = Filename(getClass.getResource(dir).toString + "*")
  //    val edgeHeader = Filename(getClass.getResource(dir + "edges1.csv").getFile)
  //    val edgeSourceFieldName = "srcVertexId"
  //    val edgeDestFieldName = "dstVertexId"
  //    val delimiter = "|"
  //    val data = ImportEdgeListWithNumericIDs(
  //      CSV(edgeCSVs, delimiter, ImportUtil.header(edgeHeader)),
  //      edgeSourceFieldName, edgeDestFieldName).result
  //    val vertices = data.vertices.rdd
  //    val edges = data.edges.rdd
  //    assert(vertices.count === 6)
  //    assert(edges.count === 8)
  //    val comments = data.attrs("comment").rdd
  //    assert(TestUtils.RDDToSortedString(edges.join(comments).values) ==
  //      """|(Edge(0,1),Adam loves Eve)
  //         |(Edge(1,0),Eve loves Adam)
  //         |(Edge(10,11),Voldemort loves Harry)
  //         |(Edge(11,10),Harry loves Voldemort)
  //         |(Edge(2,0),Bob envies Adam)
  //         |(Edge(2,1),Bob loves Eve)
  //         |(Edge(2,21),Bob loves Darth Vader)
  //         |(Edge(21,0),Darth Vader envies Adam)""".stripMargin)
  //  }

  test("import graph from csv with non-numerical IDs") {
    val dir = "/graph_operations/ImportGraphTest/non-num-ids/"
    val csv = Filename(getClass.getResource(dir + "edges.csv").getFile)
    val edgeSourceFieldName = "srcVertexId"
    val edgeDestFieldName = "dstVertexId"
    val delimiter = "|"
    val skipFirstRow = true
    val data = ImportEdgeList(
      CSV(csv, delimiter, ImportUtil.header(csv)),
      edgeSourceFieldName, edgeDestFieldName).result
    val vs = data.vertices.rdd
    val es = data.edges.rdd
    val names = data.stringID.rdd
    val comments = data.attrs("comment").rdd
    val bySrc = es.map { case (e, Edge(s, d)) => s -> (e, d) }
    val byDst = bySrc.join(names).map { case (s, ((e, d), ns)) => d -> (e, ns) }
    val named = byDst.join(names).map { case (d, ((e, ns), nd)) => e -> (ns, nd) }
    assert(TestUtils.RDDToSortedString(named.join(comments).values) ==
      """|((Bob,Darth Vader),Bob loves Darth Vader)
         |((Darth Vader,Adam),Darth Vader envies Adam)
         |((Harry,Voldemort),Harry loves Voldemort)
         |((Voldemort,Harry),Voldemort loves Harry)""".stripMargin)
  }

  test("Splitting with quoted delimiters") {
    val input = """ "Hello, ""mr, smith""!", How are you "doing"?, "Okay, thanks." """.trim
    assert(ImportUtil.split(input, delimiter = ", ") ==
      Seq("Hello, \"mr, smith\"!", "How are you \"doing\"?", "Okay, thanks."))
  }

  test("Javascript filtering") {
    val dir = "/graph_operations/ImportGraphTest/non-num-ids/"
    val path = Filename(getClass.getResource(dir + "edges.csv").getFile)
    val csv = CSV(
      path,
      "|",
      ImportUtil.header(path),
      Javascript("comment.indexOf('loves') != -1"))
    val comments = csv.lines(sparkContext).map(_(2))
    assert(TestUtils.RDDToSortedString(comments) ==
      """|Bob loves Darth Vader
         |Harry loves Voldemort
         |Voldemort loves Harry""".stripMargin)
  }
}
