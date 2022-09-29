package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class CreateScalarTest extends AnyFunSuite with TestGraphOp {
  test("CreateStringScalar") {
    val op = CreateStringScalar("hello world")
    assert(op().result.created.value == "hello world")
  }
}
