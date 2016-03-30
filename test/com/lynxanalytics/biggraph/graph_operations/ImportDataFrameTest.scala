package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.commons.lang.ClassUtils
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.sql.DataFrame

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.SQLExport

class ImportDataFrameTest extends FunSuite with TestGraphOp {

  def assertWithType[T: TypeTag](data: ImportDataFrame.Output, name: String, values: T*) {
    implicit val ct = RuntimeSafeCastable.classTagFromTypeTag[T]
    val rdd = data.columns(name).entity.runtimeSafeCast[T].rdd
    val fetched = rdd.values.collect
    val classOfT = ClassUtils.primitiveToWrapper(ct.runtimeClass)
    for (value <- fetched) {
      assert(value.getClass == classOfT)
    }
    assert(fetched.toSet == values.toSet)
  }

  test("dataframe import using JDBC works") {
    val url = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    val connection = sql.DriverManager.getConnection(url)
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
    assertWithType(data, "n", "A", "B", "C")
    assertWithType(data, "id", 1L, 2L, 3L, 4L)
    assertWithType(data, "name", "Daniel", "Beata", "Felix")
    assertWithType(data, "iq", 222.0, 222.3, 222.9)
    assertWithType(data, "race condition", "Halfling", "Dwarf", "Gnome")
    assertWithType(data, "level", 10.0, 20.0)
  }

  test("Tuple datatype export+import") {
    val g = ExampleGraph()().result
    val export = SQLExport(
      "example graph",
      g.vertices,
      Map[String, Attribute[_]](
        "location" -> g.location, "name" -> g.name))
    val dataFrame = export.dataFrameForTesting
    val data = ImportDataFrame(dataFrame).result
    assertWithType(
      data, "name",
      "Isolated Joe", "Adam", "Bob", "Eve")
    assertWithType[(Double, Double)](
      data, "location",
      (47.5269674d, 19.0323968d),
      (-33.8674869d, 151.2069902d),
      (40.71448d, -74.00598d),
      (1.352083d, 103.819836d))
  }
}
