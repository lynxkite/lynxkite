package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MetaGraphTest extends FunSuite with TestGraphOp {
  test("metagraph gives the right vertices") {
    val g = ExampleGraph()().result
    val op = OutDegree()
    val out = op(op.es, g.edges).result.outDegree
    val mg = MetaGraph(timestamp = "1", Some(this)).result
    assert(mg.vName.rdd.values.collect.toSet ==
      Set(
        "MetaGraph", "vs", "vName", "vProgress", "vGUID", "vKind", "es", "es-idSet", "eName",
        "eKind",
        "OutDegree", "outDegree",
        "ExampleGraph", "weight", "name", "location", "age", "income", "comment", "greeting",
        "edges", "edges-idSet", "vertices", "gender"))
  }
}
