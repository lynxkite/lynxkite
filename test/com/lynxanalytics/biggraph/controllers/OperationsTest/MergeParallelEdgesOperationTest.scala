package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeParallelEdgesOperationTest extends OperationsTestBase {
  test("merge parallel edges by attribute works for String") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "filter" -> ""))
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> ""
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
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges-double.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "filter" -> ""))
    run("Edge attribute to double", Map("attr" -> "call"))
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> ""
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
    // ,6.0 // Mary->John, 6.0 - duplicate

    ))
  }

  test("Merge parallel edges works") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "filter" -> ""))
    run("Merge parallel edges", Map(
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> "count"
    ))
    val call = project.edgeAttributes("call_count").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(3.0, 5.0))
  }

}
