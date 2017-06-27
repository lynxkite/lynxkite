package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.apache.spark
import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.JDBCUtil

object ImportDataFrameTest {
  def jdbcDF(dm: DataManager): spark.sql.DataFrame = {
    val url = s"jdbc:sqlite:${dm.repositoryPath.resolvedNameWithNoCredentials}/test-db"
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

    dm.sparkSession.read.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> "manytypes"))
      .load()
  }
}
class ImportDataFrameTest extends FunSuite with TestGraphOp {
  test("dataframe import using JDBC works") {
    val df = ImportDataFrameTest.jdbcDF(dataManager)
    val t = ImportDataFrame.run(df)
    assert(df.collect.toSeq == t.df.collect.toSeq)
  }
}
