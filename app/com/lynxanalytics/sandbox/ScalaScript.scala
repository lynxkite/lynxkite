package com.lynxanalytics.sandbox

import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }
import java.lang.Thread.currentThread

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain
import scala.util.DynamicVariable

class ScalaScriptSecurityManager extends SecurityManager {
  // These are temporary debug tools
  private val logs = scala.collection.mutable.Queue[String]()

  def log(entries: String*): Unit = synchronized {
    val head = Seq[String](Thread.currentThread.getName, Thread.currentThread.getId.toString)
    val entry = (head ++ entries).mkString(" ")
    logs += entry
  }
  def dump(pattern: String = ""): Unit = synchronized {
    logs.filter(_.contains(pattern)).foreach(println)
  }

  val shouldCheck = new DynamicVariable[Boolean](false)
  def checkedRun[R](restricted: Boolean, op: => R): R = {
    shouldCheck.withValue(restricted) {
      op
    }
  }

  override def getThreadGroup: ThreadGroup = {
    log("getThreadGroup called")
    if (shouldCheck.value) {
      throw new java.security.AccessControlException("Access error: GET_THREAD_GROUP")
    } else {
      super.getThreadGroup
    }
  }
  // Other permissions seem to be handled properly by the default SecurityManager
  override def checkPermission(permission: Permission): Unit = {
    log("checkPermisson: ", permission.toString)
    if (shouldCheck.value && !permission.toString.contains(".sbt")) {
      super.checkPermission(permission)
    }
  }

  override def checkPackageAccess(s: String): Unit = {
    super.checkPackageAccess(s)
    log("checkingPackageAccess: ", s)
    if (shouldCheck.value) {
      if (s.startsWith("com.lynxanalytics.biggraph")) {
        throw new java.security.AccessControlException("Illegal package access")
      }
    }
  }
}

object ScalaScript {
  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
  private val timeout = Duration(10L, "second")
  // There can only be one restricted thread
  private val executionContext = ThreadUtil.limitedExecutionContext("RestrictedScalaScript", 5)

  private val settings = engine.settings
  settings.usejavacp.value = true
  settings.embeddedDefaults[ScalaScriptSecurityManager]

  def run(code: String, restricted: Boolean = true, dump: Boolean = false): String = {
    // IMAIN.compile changes the class loader and does not restore it.
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    val result = try {
      run_inner(code, restricted, dump)
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
    }
    result
  }

  def run_inner(code: String, restricted: Boolean, dump: Boolean): String = {
    val compiledCode = engine.compile(code)
    val sm = new ScalaScriptSecurityManager
    assert(System.getSecurityManager == null)
    System.setSecurityManager(sm)
    val result = try {
      SafeFuture {
        sm.checkedRun(restricted, compiledCode.eval()).toString
      }(executionContext).awaitResult(timeout)
    } finally {
      System.setSecurityManager(null)
      if (dump) {
        sm.dump("RestrictedScalaScript")
      }
    }
    result
  }
}
