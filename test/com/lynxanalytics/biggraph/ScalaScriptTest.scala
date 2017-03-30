package com.lynxanalytics.biggraph

import org.scalatest.FunSuite
import java.net.URL
import java.security.AccessControlException

import scala.reflect.Manifest

class ScalaScriptTest extends FunSuite {

  def worksUnlessRestricted(code: String): String = {
    val result = ScalaScript.run(code, restricted = false)
    intercept[AccessControlException] {
      ScalaScript.run(code, restricted = true)
    }
    result
  }

  /*
  test("Can't do infinite loop, even when non-restricted") {
    val code =
      """
        Thread.sleep(15000L)
      """
    intercept[java.util.concurrent.TimeoutException] {
      ScalaScript.run(code, restricted = false)
    }
    intercept[java.util.concurrent.TimeoutException] {
      ScalaScript.run(code, restricted = true)
    }
  }

  test("Simple arithmetic works") {
    val code = "5 * 5 + 1"
    assert(ScalaScript.run(code, restricted = false) == "26")
    assert(ScalaScript.run(code, restricted = true) == "26")
  }

  test("Security manager disables file access") {
    val testFile: URL = getClass.getResource("/graph_api/permission_check.txt")
    val contents = "This file is used to check the security manager implementation.\n"
    assert(scala.io.Source.fromFile(testFile.getFile).mkString == contents)
    val path = testFile.getPath
    val code = s"""scala.io.Source.fromFile("${path}").mkString"""
    assert(worksUnlessRestricted(code) == contents)

  }

  test("Can't replace the security manager") {
    val code = s"""System.setSecurityManager(null)"""
    worksUnlessRestricted(code)
  }

  test("Can't tamper with our security manager") {
    val code =
      """
val s = System.getSecurityManager.asInstanceOf[com.lynxanalytics.biggraph.ScalaScriptSecurityManager]
s.disableCurrentThread
      """
    worksUnlessRestricted(code)
  }

  test("Can do some non-trivial, innocent computation") {
    val code =
      """
           class C {
             def compute(): Long = {
               val id = Thread.currentThread.getId
               val timestamp = com.lynxanalytics.biggraph.graph_util.Timestamp.toString.toLong
val timestamp = 10000000000L
               id + timestamp
             }
           }
           //val r = new C()
           //r.compute()
           1213344L
      """
    val ts = com.lynxanalytics.biggraph.graph_util.Timestamp.toString.toLong
    val result = worksUnlessRestricted(code)
    assert(result.toLong >= ts)
  }

  test("Can't create a new thread") {
    val code =
      """
           class EvilRun extends Runnable {
             override def run(): Unit = {
             }
           }
             val r = new EvilRun()
             val t = new java.lang.Thread(r)
             t.start()

      """
    worksUnlessRestricted(code)
  }
  */

  ignore("haha") {
    val code = "com.lynxanalytics.biggraph.graph_util.Timestamp.toString"
    println(ScalaScript.run(code, restricted = true, dump = true))
  }

  test("Can do some non-trivial, innocent computation") {
    val code =
      """
           class C {
             def compute(): Long = {
             val id = 0L
           //    val id = Thread.currentThread.getId
           //    val timestamp = com.lynxanalytics.biggraph.graph_util.Timestamp.toString.toLong
val timestamp = 10000000000L
               id + timestamp
             }
           }
           val r = new C()
           r.compute()
      """
    ScalaScript.run(code, restricted = true, dump = true)
  }
}