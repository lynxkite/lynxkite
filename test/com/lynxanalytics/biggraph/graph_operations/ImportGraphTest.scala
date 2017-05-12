package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.PrefixRepository
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

@deprecated("Replaced by table-based importing.", "1.7.0")
class ImportGraphTest extends FunSuite with TestGraphOp {
  val resDir = "/graph_operations/ImportGraphTest"
  val res = getClass.getResource(resDir).toString
  PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", res)
  test("import testgraph as csv from separate vertex, edge, vertexheader and edgeheader files") {
    val dir = "IMPORTGRAPHTEST$/testgraph/"
    val vertexCSVs = HadoopFile(dir + "vertex-data/part-00000")
    val edgeCSVs = HadoopFile(dir + "edge-data/part-00000")
    val vertexHeader = HadoopFile(dir + "vertex-header")
    val edgeHeader = HadoopFile(dir + "edge-header")
    val vertexIdFieldName = "vertexId"
    val sourceEdgeFieldName = "srcVertexId"
    val destEdgeFieldName = "dstVertexId"
    val delimiter = ","
    val vertexData = ImportVertexList(
      CSV(vertexCSVs, delimiter, ImportUtil.header(vertexHeader))).result
    val vid = vertexData.attrs("vertexId")
    val edgeData = {
      val op = ImportEdgeListForExistingVertexSet(
        CSV(edgeCSVs, delimiter, ImportUtil.header(edgeHeader)),
        sourceEdgeFieldName, destEdgeFieldName)
      op(op.srcVidAttr, vid)(op.dstVidAttr, vid).result
    }
    val names = vertexData.attrs("name").rdd
    assert(TestUtils.RDDToSortedString(vid.rdd.join(names).values) ==
      """|(0,Adam)
         |(1,Eve)
         |(2,Bob)""".stripMargin)
    val edges = edgeData.edges.rdd
    val bySrc = edges.map { case (e, Edge(s, d)) => s -> (e, d) }
    val byDst = bySrc.join(vid.rdd).map { case (s, ((e, d), ns)) => d -> (e, ns) }
    val namedEdges = byDst.join(vid.rdd).map { case (d, ((e, ns), nd)) => e -> (ns, nd) }
    val comments = edgeData.attrs("comment").rdd
    assert(TestUtils.RDDToSortedString(namedEdges.join(comments).values) ==
      """|((0,1),Adam loves Eve)
         |((1,0),Eve loves Adam)
         |((2,0),Bob envies Adam)
         |((2,1),Bob loves Eve)""".stripMargin)
  }

  test("Globs work with header") {
    val dir = "IMPORTGRAPHTEST$/two-edge-csv/"
    val edgeHeaderNoGlob = HadoopFile(dir + "edges1.csv")
    val headerLine1 = ImportUtil.header(edgeHeaderNoGlob)
    val edgeHeaderGlob = HadoopFile(dir + "edges1.*")
    val headerLine2 = ImportUtil.header(edgeHeaderGlob)
    assert(headerLine1 == headerLine2)

    val dirFile = HadoopFile(dir)
    intercept[Throwable] {
      ImportUtil.header(dirFile)
    }
    val nonExistentFile = HadoopFile(dir + "not_existent")
    intercept[Throwable] {
      ImportUtil.header(nonExistentFile)
    }
    val nonExistentGlob = HadoopFile(dir + "not_existent.*")
    intercept[Throwable] {
      ImportUtil.header(nonExistentGlob)
    }
  }

  test("import graph from csv as two edge files including header") {
    // different separator, no quotes around strings, newline at eof, files with wildcard
    val dir = "IMPORTGRAPHTEST$/two-edge-csv/"

    val edgeCSVs = HadoopFile(dir + "*")
    val edgeHeader = HadoopFile(dir + "edges1.csv")
    val edgeSourceFieldName = "srcVertexId"
    val edgeDestFieldName = "dstVertexId"
    val delimiter = "|"
    val data = ImportEdgeList(
      CSV(edgeCSVs, delimiter, ImportUtil.header(edgeHeader)),
      edgeSourceFieldName, edgeDestFieldName).result
    val vs = data.vertices.rdd
    val es = data.edges.rdd
    assert(vs.count === 6)
    assert(es.count === 8)
    val names = data.stringId.rdd
    val bySrc = es.map { case (e, Edge(s, d)) => s -> (e, d) }
    val byDst = bySrc.join(names).map { case (s, ((e, d), ns)) => d -> (e, ns) }
    val named = byDst.join(names).map { case (d, ((e, ns), nd)) => e -> (ns, nd) }
    val comments = data.attrs("comment").rdd
    assert(TestUtils.RDDToSortedString(named.join(comments).values) ==
      """|((0,1),Adam loves Eve)
         |((1,0),Eve loves Adam)
         |((10,11),Voldemort loves Harry)
         |((11,10),Harry loves Voldemort)
         |((2,0),Bob envies Adam)
         |((2,1),Bob loves Eve)
         |((2,21),Bob loves Darth Vader)
         |((21,0),Darth Vader envies Adam)""".stripMargin)
  }

  test("import graph from csv with non-numerical IDs") {
    val dir = "IMPORTGRAPHTEST$/non-num-ids/"
    val csv = HadoopFile(dir + "edges.csv")
    val edgeSourceFieldName = "srcVertexId"
    val edgeDestFieldName = "dstVertexId"
    val delimiter = "|"
    val skipFirstRow = true
    val data = ImportEdgeList(
      CSV(csv, delimiter, ImportUtil.header(csv)),
      edgeSourceFieldName, edgeDestFieldName).result
    val vs = data.vertices.rdd
    val es = data.edges.rdd
    val names = data.stringId.rdd
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

  test("Splitting edge cases") {
    assert(ImportUtil.split("", delimiter = ",") == Seq(""))
    assert(ImportUtil.split("a", delimiter = ",") == Seq("a"))
    assert(ImportUtil.split(",", delimiter = ",") == Seq("", ""))
    assert(ImportUtil.split("a,", delimiter = ",") == Seq("a", ""))
    assert(ImportUtil.split("a,,", delimiter = ",") == Seq("a", "", ""))
    assert(ImportUtil.split(",a,b", delimiter = ",") == Seq("", "a", "b"))
    assert(ImportUtil.split(",,a,b,", delimiter = ",") == Seq("", "", "a", "b", ""))
  }

  test("Splitting with quoted delimiters") {
    val input = """ "Hello, ""mr, smith""!", How are you "doing"?, "Okay, thanks." """.trim
    assert(ImportUtil.split(input, delimiter = ", ") ==
      Seq("Hello, \"mr, smith\"!", "How are you \"doing\"?", "Okay, thanks."))
  }

  test("JavaScript filtering") {
    val dir = "IMPORTGRAPHTEST$/non-num-ids/"
    val path = HadoopFile(dir + "edges.csv")
    val csv = CSV(
      path,
      "|",
      ImportUtil.header(path),
      filter = JavaScript("comment.indexOf('loves') != -1"))
    val comments = csv.lines(dataManager.runtimeContext).values.map(_(2))
    assert(TestUtils.RDDToSortedString(comments) ==
      """|Bob loves Darth Vader
         |Harry loves Voldemort
         |Voldemort loves Harry""".stripMargin)
  }

  test("Dropping columns") {
    val dir = "IMPORTGRAPHTEST$/non-num-ids/"
    val path = HadoopFile(dir + "edges.csv")
    val csv = CSV(
      path,
      "|",
      ImportUtil.header(path),
      omitFields = Set("comment"))
    assert(csv.fields == Seq("srcVertexId", "dstVertexId"))
    val stuff = csv.lines(dataManager.runtimeContext).values
    assert(TestUtils.RDDToSortedString(stuff) ==
      """|List(Bob, Darth Vader)
         |List(Darth Vader, Adam)
         |List(Harry, Voldemort)
         |List(Voldemort, Harry)""".stripMargin)
  }

  test("import from non-existent file throws AssertionError") {

    intercept[AssertionError] {
      ImportEdgeList(CSV(HadoopFile("DATA$/non-existent"), ",", "src,dst"), "src", "dst").result.edges.rdd
    }
    intercept[AssertionError] {
      ImportEdgeList(CSV(HadoopFile("DATA$/non-existent/*"), ",", "src,dst"), "src", "dst").result.edges.rdd
    }
  }
}
