package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.commons.lang.ClassUtils
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.JDBCUtil

class TableToAttributesTest extends FunSuite with TestGraphOp {
  test("dataframe import using JDBC works") {
    val df = ImportDataFrameTest.jdbcDF(dataManager)
    val t = ImportDataFrame.run(df)
    val data = TableToAttributes.run(t)

    assert(data.ids.rdd.count == 5)

    def assertWithType[T: TypeTag](name: String, values: T*) {
      implicit val ct = RuntimeSafeCastable.classTagFromTypeTag[T]
      val rdd = data.columns(name).entity.runtimeSafeCast[T].rdd
      assert(rdd.count == values.size)
      val fetched = rdd.values.collect
      val classOfT = ClassUtils.primitiveToWrapper(ct.runtimeClass)
      for (value <- fetched) {
        assert(value.getClass == classOfT)
      }
      assert(fetched.toSet == values.toSet)
    }

    assertWithType("n", "A", "B", "C")
    assertWithType("id", 1L, 2L, 3L, 4L)
    assertWithType("name", "Daniel", "Beata", "Felix")
    assertWithType("iq", 222.0, 222.3, 222.9)
    assertWithType("race condition", "Halfling", "Dwarf", "Gnome")
    assertWithType("level", 10.0, 20.0)
  }
}
