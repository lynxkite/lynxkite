package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DBImportTest extends FunSuite with TestGraphOp {
  test("import vertex list from SQLite") {
    val db = s"sqlite:${dataManager.repositoryPath.resolvedName}/test-db"
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers
      (id INTEGER, name TEXT, gender TEXT, "race condition" TEXT, level DOUBLE PRECISION);
    INSERT INTO subscribers VALUES
      (1, 'Daniel', 'Male', 'Halfling', 10.0),
      (2, 'Beata', 'Female', 'Dwarf', 20.0),
      (3, 'Felix', 'Male', 'Gnome', NULL);
    """)
    connection.close()
    val data = ImportVertexList(
      DBTable(db, "subscribers", Seq("id", "name", "race condition", "level"), "id")).result
    assert(data.attrs.keySet == Set("id", "name", "race condition", "level"))
    assert(data.vertices.rdd.count == 3)
    val names = data.attrs("name").rdd
    val races = data.attrs("race condition").rdd
    val levels = data.attrs("level").rdd
    val racesByNames = names.sortedJoin(races).values.collect.toSeq.sorted
    assert(racesByNames == Seq("Beata" -> "Dwarf", "Daniel" -> "Halfling", "Felix" -> "Gnome"))
    val levelsByNames = names.sortedJoin(levels).values.collect.toSeq.sorted
    assert(levelsByNames == Seq("Beata" -> "20.0", "Daniel" -> "10.0"))
  }
}
