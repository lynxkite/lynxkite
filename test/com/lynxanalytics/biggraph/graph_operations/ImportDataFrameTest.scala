package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.commons.lang.ClassUtils
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.JDBCUtil

class ImportDataFrameTest extends FunSuite with TestGraphOp {
  test("dataframe import using JDBC works") {
    val url = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    val connection = JDBCUtil.getConnection(url)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS manytypes;
    CREATE TABLE manytypes
      (n TEXT, id INTEGER, name TEXT, iq REAL, "race condition" TEXT, level DOUBLE PRECISION);
    INSERT INTO manytypes VALUES
      ('A', 1, 'Daniel', 222.0, 'Halfling', 10.0),
      ('B', 2, 'Beata', 222.3, 'Dwarf', 20.0),
      ('C', 3, 'Felix', 222.9, 'Gnome', NULL),
      (NULL, 4, NULL, NULL, NULL, NULL),
      (NULL, NULL, NULL, NULL, NULL, NULL);
    """)
    connection.close()

    val df =
      dataManager.newSQLContext().read.format("jdbc")
        .options(Map("url" -> url, "dbtable" -> "manytypes"))
        .load()

    val data = ImportDataFrame(df).result

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
