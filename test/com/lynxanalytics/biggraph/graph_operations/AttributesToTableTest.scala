package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.commons.lang.ClassUtils
import org.apache.spark
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.JDBCUtil

class AttributesToTableTest extends FunSuite with TestGraphOp {
  private def toSeq(row: spark.sql.Row): Seq[Any] = {
    row.toSeq.map {
      case r: spark.sql.Row => toSeq(r)
      case x => x
    }
  }

  test("example graph") {
    val eg = ExampleGraph()().result
    val t = AttributesToTable.run(eg.vertexAttributes.mapValues(_.entity))
    val data = t.df.collect.toSeq.map(row => toSeq(row))
    assert(data == Seq(
      Seq("Adam", Seq(40.71448, -74.00598), 20.3, 1000.0, "Male"),
      Seq("Eve", Seq(47.5269674, 19.0323968), 18.2, null, "Female"),
      Seq("Bob", Seq(1.352083, 103.819836), 50.3, 2000.0, "Male"),
      Seq("Isolated Joe", Seq(-33.8674869, 151.2069902), 2.0, null, "Male")))
  }
}
