package com.lynxanalytics.biggraph

import org.scalatest.FunSuite
import java.net.URL
import java.security.AccessControlException

class ScalaScriptTest extends FunSuite {

  test("Can't do infinite loop") {
    intercept[java.util.concurrent.TimeoutException] {
      ScalaScript.run(
        """
        Thread.sleep(10000L);
      """)
    }
  }

  test("Simple arithmetic works") {
    assert(ScalaScript.run("5 * 5 + 1") == "26")
  }

  test("Security manager disables file access") {
    val testFile: URL = getClass.getResource("/graph_api/permission_check.txt")
    assert(scala.io.Source.fromFile(testFile.getFile).mkString
      == "This file is used to check the security manager implementation.\n")
    val path = testFile.getPath
    intercept[AccessControlException] {
      ScalaScript.run(s"""scala.io.Source.fromFile("${path}").mkString""")
    }
  }

}