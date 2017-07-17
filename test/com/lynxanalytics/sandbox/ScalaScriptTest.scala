package com.lynxanalytics.sandbox

import org.scalatest.FunSuite
import java.security.AccessControlException

import com.lynxanalytics.biggraph.controllers.SQLController
import com.lynxanalytics.biggraph.graph_api.{ Scripting, Table, TestGraphOp }
import com.lynxanalytics.biggraph.graph_operations.ImportDataFrameTest
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe._

class ScalaScriptTest extends FunSuite with TestGraphOp {

  test("Can't do infinite loop") {
    val code =
      """
        Thread.sleep(3000L)
      """
    intercept[java.util.concurrent.TimeoutException] {
      ScalaScript.run(code, Map(), 2L)
    }
  }

  test("Simple arithmetic works") {
    val code = "5 * 5 + 1"

    assert(ScalaScript.run(code) == "26")
  }

  test("Bindings work") {
    assert(ScalaScript.run("s\"\"\"asd $qwe\"\"\"", Map("qwe" -> "123")) == "asd 123")
  }

  test("Scala DataFrame bindings work with runVegas") {
    val df = ImportDataFrameTest.jdbcDF(dataManager)
    val code = """
    Vegas("My plot test")
      .withData(table)
      .encodeX("name", Nominal)
      .encodeY("level", Quantitative)
      .mark(Bar)
      """
    val JSONString = ScalaScript.runVegas(code, df)
    assert(JSONString contains """"mark" : "bar"""")
    assert(JSONString contains "encoding")
    assert(JSONString contains "description")
    assert(JSONString contains "My plot test")
    assert(JSONString contains """"name" : "Felix",""")
  }

  test("Scala multiline string works in plot code") {
    val df = ImportDataFrameTest.jdbcDF(dataManager)
    val filterRule = """datum.b > 20 &&
    datum.b < 60"""
    val code = s"""
    Vegas("Plot test with multiline string")
      .withData(
        Seq(
          Map("a" -> "A", "b" -> 28), Map("a" -> "B", "b" -> 55), Map("a" -> "C", "b" -> 43),
          Map("a" -> "D", "b" -> 91), Map("a" -> "E", "b" -> 81), Map("a" -> "F", "b" -> 53),
          Map("a" -> "G", "b" -> 19), Map("a" -> "H", "b" -> 87), Map("a" -> "I", "b" -> 52)))
      .encodeX("a", Nominal)
      .encodeY("b", Quantitative)
      .mark(Bar)
      .filter(\"\"\"$filterRule\"\"\")
      """
    val JSONString = ScalaScript.runVegas(code, df)
    println(JSONString)
  }

  test("Security manager disables file access") {
    val testFile = getClass.getResource("/graph_api/permission_check.txt")
    val contents = "This file is used to check the security manager implementation.\n"
    assert(scala.io.Source.fromFile(testFile.getFile).mkString == contents)
    val path = testFile.getPath
    val code = s"""scala.io.Source.fromFile("${path}").mkString"""
    intercept[AccessControlException] {
      ScalaScript.run(code)
    }
  }

  test("Can't replace the security manager") {
    val code = "System.setSecurityManager(null)"
    intercept[AccessControlException] {
      ScalaScript.run(code)
    }
  }

  test("Can do some non-trivial, innocent computation") {
    val code =
      """
           class C(val str: String) {
             def compute(): String = {
                str + "lo"
             }
           }
           val r = new C("hel")
           r.compute()
      """
    assert(ScalaScript.run(code) == "hello")
  }

  test("Can't create a new thread") {
    val code =
      """
           class EvilRun extends Runnable {
             override def run(): Unit = {
             // We could do anything here
             }
           }
             val r = new EvilRun()
             val t = new java.lang.Thread(r)
             t.start()

      """
    intercept[AccessControlException] {
      ScalaScript.run(code)
    }
  }

  test("Can't access biggraph classes") {
    val code = "com.lynxanalytics.biggraph.graph_util.Timestamp.toString"
    intercept[AccessControlException] {
      ScalaScript.run(code)
    }
  }

  test("Infer type") {
    val t1 = ScalaScript.compileAndGetType(
      "a + b", Map("a" -> typeTag[String], "b" -> typeTag[String]), paramsToOption = false)
    assert(t1.funcType.tpe =:= typeOf[(String, String) => String])
    assert(!t1.isOptionType)
    assert(t1.payloadType =:= typeOf[String])

    val t2 = ScalaScript.compileAndGetType(
      "Vector(a)", Map("a" -> typeTag[String]), paramsToOption = false)
    assert(t2.funcType.tpe =:= typeOf[String => scala.collection.immutable.Vector[String]])
    assert(!t2.isOptionType)
    assert(t2.payloadType =:= typeOf[scala.collection.immutable.Vector[String]])

    val t3 = ScalaScript.compileAndGetType(
      "a ++ Vector(\"x\")", Map("a" -> typeTag[Vector[String]]), paramsToOption = false)
    assert(t3.funcType.tpe =:= typeOf[Vector[String] => scala.collection.immutable.Vector[String]])
    assert(!t3.isOptionType)
    assert(t3.payloadType =:= typeOf[scala.collection.immutable.Vector[String]])
  }

  test("Infer Option type") {
    val ot1 = ScalaScript.compileAndGetType("a", Map("a" -> typeTag[String]), paramsToOption = true)
    assert(ot1.isOptionType)
    assert(ot1.funcType.tpe =:= typeOf[Option[String] => Option[String]])
    assert(ot1.payloadType =:= typeOf[String])

    val ot2 = ScalaScript.compileAndGetType("Some(1.0)", Map(), paramsToOption = false)
    assert(ot2.isOptionType)
    assert(ot2.funcType.tpe =:= typeOf[() => Some[Double]])
    assert(ot2.payloadType =:= typeOf[Double])

    val ot3 = ScalaScript.compileAndGetType(
      "if (a) Some(1.0) else None", Map("a" -> typeTag[Boolean]), paramsToOption = false)
    assert(ot3.isOptionType)
    assert(ot3.funcType.tpe =:= typeOf[Boolean => Option[Double]])
    assert(ot3.payloadType =:= typeOf[Double])
  }
}
