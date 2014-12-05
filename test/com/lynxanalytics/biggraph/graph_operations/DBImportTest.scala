package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DBImportTest extends FunSuite with TestGraphOp {
  test("import vertex list from SQLite") {
    val db = s"sqlite:${dataManager.repositoryPath}/test-db"
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers (id INTEGER, name TEXT, gender TEXT, race TEXT);
    INSERT INTO subscribers VALUES
      (1, 'Daniel', 'Male', 'Halfling'),
      (2, 'Beata', 'Female', 'Dwarf'),
      (3, 'Felix', 'Male', 'Gnome');
    """)
    connection.close()
    val data = ImportVertexList(
      DBTable(db, "subscribers", Seq("id", "name", "race"), "id", 0, 10)).result
    assert(data.attrs.keySet == Set("id", "name", "race"))
    assert(data.vertices.rdd.count == 3)
    val names = data.attrs("name").rdd
    val genders = data.attrs("race").rdd
    val gendersByNames = names.sortedJoin(genders).values.collect.toSeq.sorted
    assert(gendersByNames == Seq("Beata" -> "Dwarf", "Daniel" -> "Halfling", "Felix" -> "Gnome"))
  }
}
