package com.lynxanalytics.biggraph.graph_operations

import java.sql
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class DBImportTest extends FunSuite with TestGraphOp {
  test("import vertex list from SQLite") {
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers
      (n TEXT, id INTEGER, name TEXT, gender TEXT, "race condition" TEXT, level DOUBLE PRECISION);
    INSERT INTO subscribers VALUES
      ('A', 1, 'Daniel', 'Male', 'Halfling', 10.0),
      ('B', 2, 'Beata', 'Female', 'Dwarf', 20.0),
      ('C', 3, 'Felix', 'Male', 'Gnome', NULL),
      (NULL, 4, NULL, NULL, NULL, NULL);
    """)
    connection.close()
    val data = ImportVertexList(
      DBTable(
        db, "subscribers", Seq("n", "id", "name", "race condition", "level"), "id")).result
    assert(data.attrs.keySet == Set("n", "id", "name", "race condition", "level"))
    assert(data.vertices.rdd.count == 4)
    val names = data.attrs("name").rdd
    val races = data.attrs("race condition").rdd
    val levels = data.attrs("level").rdd
    val racesByNames = names.sortedJoin(races).values.collect.toSeq.sorted
    assert(racesByNames == Seq("Beata" -> "Dwarf", "Daniel" -> "Halfling", "Felix" -> "Gnome"))
    val levelsByNames = names.sortedJoin(levels).values.collect.toSeq.sorted
    assert(levelsByNames == Seq("Beata" -> "20.0", "Daniel" -> "10.0"))
  }
  test("import edges from SQLite") {
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    val connection = sql.DriverManager.getConnection("jdbc:" + db)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS edges;
    CREATE TABLE edges
      (id INTEGER, src TEXT, dst TEXT, weight DOUBLE PRECISION);
    INSERT INTO edges VALUES
      (1, 'A', 'B', 10.0),
      (2, 'B', 'C', 20.0),
      (3, NULL, 'D', 42.0);
    """)
    connection.close()
    val data = ImportEdgeList(
      DBTable(db, "edges", Seq("id", "src", "dst", "weight"), "id"), "src", "dst").result
    assert(data.attrs.keySet == Set("id", "src", "dst", "weight"))
    assert(data.vertices.rdd.count == 3)
    assert(data.edges.rdd.count == 2)
    val weight = data.attrs("weight").rdd
    assert(weight.count == 2)
    val src = data.attrs("src").rdd
    val weightBySrc = src.sortedJoin(weight).values.collect.toSeq.sorted
    assert(weightBySrc == Seq("A" -> "10.0", "B" -> "20.0"))
  }
}
