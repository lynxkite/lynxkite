package com.lynxanalytics.lynxkite.frontend_operations

import scala.sys.process._
import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class Neo4jTest extends AnyFunSuite with TestGraphOp {
  before {
    val tgz = getClass.getResource("/graph_operations/neo4j-3.5.2-movie-db.tgz").getPath
    s"tar xf $tgz -C $myTempDir".!
  }
  val dbPath = s"$myTempDir/neo4j-3.5.2-movie-db/databases/graph.db"
  val ss = sparkDomain.sparkSession

  test("import all nodes") {
    val df = Neo4j.readNodes(
      ss,
      dbPath,
      Seq(),
      Seq(
        "name" -> SerializableType.string,
        "born" -> SerializableType.long,
        "title" -> SerializableType.string,
        "released" -> SerializableType.long,
      ))
    assert(df.schema.map(_.name).mkString(" ") == "_id _labels name born title released")
    val r = df.head
    assert(r(0) == 0)
    assert(r(1) == "Movie")
    assert(r(2) == null)
    assert(r(3) == null)
    assert(r(4) == "The Matrix")
    assert(r(5) == 1999)
    assert(df.count == 171)
  }

  test("import movies") {
    val df = Neo4j.readNodes(ss, dbPath, Seq("Movie"), Seq())
    assert(df.count == 38)
  }

  test("import all relationships") {
    val df = Neo4j.readRelationships(ss, dbPath, Seq(), Seq())
    assert(df.schema.map(_.name).mkString(" ") == "_id _type _src _dst")
    val r = df.take(101).last
    assert(r(0) == 100)
    assert(r(1) == "ACTED_IN")
    assert(r(2) == 80)
    assert(r(3) == 78)
    assert(df.count == 253)
  }

  test("import reviews") {
    val df = Neo4j.readRelationships(
      ss,
      dbPath,
      Seq("REVIEWED"),
      Seq(
        "rating" -> SerializableType.long,
        "summary" -> SerializableType.string,
      ))
    assert(df.schema.map(_.name).mkString(" ") == "_id _type _src _dst rating summary")
    val r = df.head
    assert(r(0) == 244)
    assert(r(1) == "REVIEWED")
    assert(r(2) == 169)
    assert(r(3) == 105)
    assert(r(4) == 95)
    assert(r(5) == "An amazing journey")
    assert(df.count == 9)
  }

  test("import an array") {
    val df = Neo4j.readRelationships(
      ss,
      dbPath,
      Seq("ACTED_IN"),
      Seq(
        "roles" -> SerializableType[Vector[String]],
      ))
    assert(df.schema.map(_.name).mkString(" ") == "_id _type _src _dst roles")
    // Tom Hanks in Polar Express, an example of multiple roles.
    val r = df.filter(df("_src") === 71).filter(df("_dst") === 161).head
    assert(r(0) == 232)
    assert(r(1) == "ACTED_IN")
    assert(r(2) == 71)
    assert(r(3) == 161)
    assert(r.getSeq[String](4) == List("Hero Boy", "Father", "Conductor", "Hobo", "Scrooge", "Santa Claus"))
    assert(df.count == 172)
  }
}
