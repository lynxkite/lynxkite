package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CreateScalarTest extends AnyFunSuite with TestGraphOp {
  test("CreateStringScalar") {
    val op = CreateStringScalar("hello world")
    assert(op().result.created.value == "hello world")
  }
}
