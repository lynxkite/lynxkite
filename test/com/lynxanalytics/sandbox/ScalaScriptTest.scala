package com.lynxanalytics.sandbox

import org.scalatest.funsuite.AnyFunSuite
import java.security.AccessControlException

import com.lynxanalytics.biggraph.graph_api.TestGraphOp
import com.lynxanalytics.biggraph.graph_operations.ImportDataFrameTest

import scala.reflect.runtime.universe._

class ScalaScriptTest extends AnyFunSuite with TestGraphOp {

  test("Simple arithmetic works") {
    val code = "5 * 5 + 1"

    assert(ScalaScript.run(code) == "26")
  }

  test("Bindings work") {
    assert(ScalaScript.run("s\"\"\"asd $qwe\"\"\"", Map("qwe" -> "123")) == "asd 123")
  }

  test("Security manager disables file access") {
    val testFile = getClass.getResource("/graph_api/permission_check.txt")
    val contents = "This file is used to check the security manager implementation.\n"
    assert(scala.io.Source.fromFile(testFile.getFile).mkString == contents)
    val path = testFile.getPath
    val code = s"""scala.io.Source.fromFile("${path}").mkString"""
    val e = intercept[Exception] {
      ScalaScript.run(code)
    }
    assert(e.getCause.isInstanceOf[AccessControlException])
  }

  test("Can't replace the security manager") {
    val code = "System.setSecurityManager(null)"
    val e = intercept[Exception] {
      ScalaScript.run(code)
    }
    assert(e.getCause.isInstanceOf[AccessControlException])
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
    val e = intercept[Exception] {
      ScalaScript.run(code)
    }
    assert(e.getCause.isInstanceOf[AccessControlException])
  }

  test("Can't access biggraph classes") {
    val code = "com.lynxanalytics.biggraph.graph_util.Timestamp.toString"
    val e = intercept[Exception] {
      ScalaScript.run(code)
    }
    assert(e.getCause.isInstanceOf[AccessControlException])
  }

  test("Infer type") {
    val t1 = ScalaScript.compileAndGetType(
      "a + b", Map("a" -> typeTag[String], "b" -> typeTag[String]))
    assert(t1.funcType.tpe =:= typeOf[(String, String) => String])
    assert(!t1.isOptionType)
    assert(t1.payloadType =:= typeOf[String])

    val t2 = ScalaScript.compileAndGetType(
      "Vector(a)", Map("a" -> typeTag[String]))
    assert(t2.funcType.tpe =:= typeOf[String => scala.collection.immutable.Vector[String]])
    assert(!t2.isOptionType)
    assert(t2.payloadType =:= typeOf[scala.collection.immutable.Vector[String]])

    val t3 = ScalaScript.compileAndGetType(
      "a ++ Vector(\"x\")", Map("a" -> typeTag[Vector[String]]))
    assert(t3.funcType.tpe =:= typeOf[Vector[String] => scala.collection.immutable.Vector[String]])
    assert(!t3.isOptionType)
    assert(t3.payloadType =:= typeOf[scala.collection.immutable.Vector[String]])
  }

  test("Infer Option type") {
    val ot1 = ScalaScript.compileAndGetType("a", Map(), Map("a" -> typeTag[String]))
    assert(ot1.isOptionType)
    assert(ot1.funcType.tpe =:= typeOf[Option[String] => Option[String]])
    assert(ot1.payloadType =:= typeOf[String])

    val ot2 = ScalaScript.compileAndGetType("Some(1.0)", Map())
    assert(ot2.isOptionType)
    assert(ot2.funcType.tpe =:= typeOf[() => Some[Double]])
    assert(ot2.payloadType =:= typeOf[Double])

    val ot3 = ScalaScript.compileAndGetType(
      "if (a) Some(1.0) else None", Map("a" -> typeTag[Boolean]))
    assert(ot3.isOptionType)
    assert(ot3.funcType.tpe =:= typeOf[Boolean => Option[Double]])
    assert(ot3.payloadType =:= typeOf[Double])
  }

  test("collect variables") {
    assert(ScalaScript.findVariables("Some(a)") == Set("<empty>", "scala", "Some", "a"))

    assert(ScalaScript.findVariables("`age`").contains("age"))
    assert(ScalaScript.findVariables("`123 weird id #?!`").contains("123 weird id #?!"))
    assert(ScalaScript.findVariables("age").contains("age"))
    assert(ScalaScript.findVariables(" age ").contains("age"))
    assert(ScalaScript.findVariables("src$age").contains("src$age"))
    assert(ScalaScript.findVariables("age - name").contains("age"))
    assert(ScalaScript.findVariables("age_v2").contains("age_v2"))
    assert(ScalaScript.findVariables("age.toString").contains("age"))
    assert(ScalaScript.findVariables("age\n1.0").contains("age"))
    assert(ScalaScript.findVariables("Name").contains("Name"))

    assert(!ScalaScript.findVariables("name").contains("nam"))
    assert(!ScalaScript.findVariables("name").contains("ame"))
    assert(!ScalaScript.findVariables("nam").contains("name"))
    assert(!ScalaScript.findVariables("ame").contains("name"))
    assert(!ScalaScript.findVariables("\"name\"").contains("name"))
    assert(!ScalaScript.findVariables("\" name \"").contains("name"))
    assert(!ScalaScript.findVariables("'name").contains("name"))
  }

  test("Recover after compiler gets confused") {
    // See #7227
    intercept[Exception] {
      ScalaScript.run("{")
    }
    ScalaScript.run("val a = 1; a")
  }

}
