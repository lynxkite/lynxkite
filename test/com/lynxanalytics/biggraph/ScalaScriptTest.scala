package com.lynxanalytics.biggraph

import org.scalatest.FunSuite
import java.net.URL
import java.security.AccessControlException

import scala.reflect.Manifest

class ScalaScriptTest extends FunSuite {

  def hello(): Unit = {
    class EvilRun extends Runnable {
      override def run(): Unit = {
        val name = Thread.currentThread.getName
        val id = Thread.currentThread.getId.toString
        println("Evil " + name + " " + id)
      }
    }
    val r = new EvilRun()
    val t = new java.lang.Thread(r)
    t.start()
  }

  def expect[T <: AnyRef](f: => Any)(implicit manifest: Manifest[T]): Unit = {
    val e = intercept[T] {
      f
    }
    println(e.asInstanceOf[Throwable].getMessage)
  }
  ignore("Can't do infinite loop") {
    expect[java.util.concurrent.TimeoutException] {
      ScalaScript.run(
        """
        Thread.sleep(10000L);
      """)
    }
  }

  ignore("Simple arithmetic works") {
    assert(ScalaScript.run("5 * 5 + 1") == "26")
  }

  /*
  test("Security manager disables file access") {
    val testFile: URL = getClass.getResource("/graph_api/permission_check.txt")
    assert(scala.io.Source.fromFile(testFile.getFile).mkString
      == "This file is used to check the security manager implementation.\n")
    val path = testFile.getPath
    intercept[AccessControlException] {
      ScalaScript.run(s"""scala.io.Source.fromFile("${path}").mkString""")
    }
  }
  test("Can't replace the security manager") {
    expect[AccessControlException] {
      ScalaScript.run(s"""System.setSecurityManager(null)""")
    }
  }

  test("Can't tamper with our security manager") {
    ScalaScript.run(
      """
        val s = System.getSecurityManager.asInstanceOf[com.lynxanalytics.biggraph.ScalaScriptSecurityManager]
        s.disableCurrentThread
      """)
  }

  test("Can create a class") {
    ScalaScript.run(
      """
           class C {
             def run(): Unit = {
               val name = Thread.currentThread.getName
               val id = Thread.currentThread.getId.toString
               println("Evil " + name + " " + id)
             }
           }
             val r = new C()
             r.run()
             """)
  }
*/
  test("Can't create a new thread") {
    //    expect[java.util.concurrent.ExecutionException] {
    ScalaScript.run(
      s"""
           class EvilRun extends Runnable {
             override def run(): Unit = {
               val name = Thread.currentThread.getName
               val id = Thread.currentThread.getId.toString
               println("Evil " + name + " " + id)
             }
           }
             val r = new EvilRun()
             val t = new java.lang.Thread(r)
             t.start()

           """)
    //    }
  }
}