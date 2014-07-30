package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CreateScalarTest extends FunSuite with TestGraphOp {
  test("CreateStringScalar") {
    val op = CreateStringScalar("hello world")
    assert(op().result.created.value == "hello world")
  }
}
