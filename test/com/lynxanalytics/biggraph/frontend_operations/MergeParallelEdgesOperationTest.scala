package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeParallelEdgesOperationTest extends OperationsTestBase {

  test("Compiles and fails") {
    assert(false)
  }

  /*
  def load(filename: String) = {
    run("Import vertices and edges from a single table", Map(
      "table" -> importCSV("OPERATIONSTEST$/" + filename),
      "src" -> "src",
      "dst" -> "dst"))
  }

  test("merge parallel edges by attribute works for String") {
    load("merge-parallel-edges.csv")
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate_src" -> "",
      "aggregate_dst" -> "",
      "aggregate_call" -> ""
    ))
    val call = project.edgeAttributes("call").runtimeSafeCast[String]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(
      "Monday", // Mary->John, Wednesday
      // "Monday",  // Mary->John, Wednesday - duplicate
      "Saturday", // Mary->John, Saturday
      //"Saturday", // Mary->John, Saturday - duplicate
      "Tuesday", // John->Mary, Tuesday
      //"Tuesday",  // John->Mary, Tuesday - duplicate
      "Wednesday", // Mary->John, Wednesday
      "Wednesday" // John->Mary, Wednesday
    ))
  }

  test("merge parallel edges by attribute works for Double") {
    load("merge-parallel-edges-double.csv")
    run("Convert edge attribute to double", Map("attr" -> "call"))
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate_src" -> "",
      "aggregate_dst" -> "",
      "aggregate_call" -> ""
    ))
    val call = project.edgeAttributes("call").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(
      1.0, // Mary->John, 1.0
      // 1.0,  // Mary->John, 1.0 - duplicate
      2.0, // John->Mary, 2.0
      // 2.0,  // John->Mary, 2.0 - duplicate
      3.0, // Mary->John, 3.0
      3.0, // John->Mary, 3.0
      6.0 // Mary->John, 6.0
    // 6.0 // Mary->John, 6.0 - duplicate
    ))
  }

  test("Merge parallel edges works") {
    load("merge-parallel-edges.csv")
    run("Merge parallel edges", Map(
      "aggregate_src" -> "",
      "aggregate_dst" -> "",
      "aggregate_call" -> "count"
    ))
    val call = project.edgeAttributes("call_count").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(3.0, 5.0))
  }

  test("Merge parallel edges with undefined values keeps the defined values") {
    load("merge-parallel-edges-double.csv")
    run("Convert edge attribute to double", Map("attr" -> "call"))
    run("Derive edge attribute", Map(
      "output" -> "call",
      "type" -> "double",
      "expr" -> "call == 6.0 ? call : undefined"))
    run("Merge parallel edges", Map(
      "aggregate_src" -> "most_common",
      "aggregate_dst" -> "most_common",
      "aggregate_call" -> "max"
    ))
    val call = project.edgeAttributes("call_max").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(6.0))
  }
  */
}
