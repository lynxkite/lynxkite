package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.commons.lang.ClassUtils
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.JDBCUtil

class AttributesToTableTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val t = AttributesToTable.run(eg.vertexAttributes.mapValues(_.entity))
    assert(t.df.select("name", "income", "gender").collect.toSeq.map(_.toSeq) == Seq(
      Seq("Adam", 1000.0, "Male"),
      Seq("Eve", null, "Female"),
      Seq("Bob", 2000.0, "Male"),
      Seq("Isolated Joe", null, "Male")))
  }
}
