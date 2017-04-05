package com.lynxanalytics.sandbox

import java.io.FilePermission
import java.net.NetPermission
import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.SafeFuture
import com.lynxanalytics.biggraph.graph_api.ThreadUtil

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain
import scala.util.DynamicVariable

class ScalaScriptSecurityManager extends SecurityManager {

  val shouldCheck = new DynamicVariable[Boolean](false)
  def checkedRun[R](restricted: Boolean, op: => R): R = {
    shouldCheck.withValue(restricted) {
      op
    }
  }

  override def getThreadGroup: ThreadGroup = {
    if (shouldCheck.value) {
      throw new java.security.AccessControlException("Access error: GET_THREAD_GROUP")
    } else {
      super.getThreadGroup
    }
  }

  // The scala class loader does stuff disallowed by the
  // SecurityManager, namely:
  // 1) Creating an URL like this: new URL(null, s"memory:${file.path}", new URLStreamHandler)
  // 2) When called in sbt test environment, it also wants to access this file:
  //   ~/.sbt/boot/scala-2.10.4/org.scala-sbt/sbt/0.13.5/test-agent-0.13.5.jar
  //   I have no idea why.
  // So, if we are called from scala.reflect.internal.util.ScalaClassLoader.classAsStream
  // we'll be a bit less restrictive, otherwise we couldn't create classes.
  // This is a bit expensive, but we'll make sure not to call it too often.
  private def calledByClassLoader: Boolean = {
    Thread.currentThread.getStackTrace.exists {
      stackTraceElement =>
        stackTraceElement.getClassName == "scala.reflect.internal.util.ScalaClassLoader$class" &&
          stackTraceElement.getMethodName == "classAsStream"
    }
  }

  override def checkPermission(permission: Permission): Unit = {
    if (shouldCheck.value) {
      permission match {
        case _: NetPermission =>
          if (!calledByClassLoader) {
            super.checkPermission(permission)
          }
        case p: FilePermission =>
          if (!(p.getActions == "read" && p.getName.contains(".sbt"))) {
            super.checkPermission(permission)
          }
        case _ =>
          super.checkPermission(permission)
      }
    }
  }

  override def checkPackageAccess(s: String): Unit = {
    super.checkPackageAccess(s) // This must be the first thing to do!
    if (shouldCheck.value &&
      (s.contains("com.lynxanalytics.biggraph") ||
        s.contains("org.apache.spark"))) {
      throw new java.security.AccessControlException("Illegal package access")
    }
  }
}

object ScalaScript {
  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
  private val timeout = Duration(10L, "second")
  private val numRestrictedThreads = 5
  private val executionContext =
    ThreadUtil.limitedExecutionContext("RestrictedScalaScript", numRestrictedThreads)

  val restrictedSecurityManager = new ScalaScriptSecurityManager
  private val settings = engine.settings
  settings.usejavacp.value = true
  settings.embeddedDefaults[ScalaScriptSecurityManager]

  def run(code: String, restricted: Boolean = true): String = {
    // IMAIN.compile changes the class loader and does not restore it.
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    val result = try {
      run_inner(code, restricted)
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
    }
    result
  }

  def run_inner(code: String, restricted: Boolean): String = {
    val compiledCode = engine.compile(code)
    assert(System.getSecurityManager == null)
    System.setSecurityManager(restrictedSecurityManager)
    val result = try {
      SafeFuture {
        restrictedSecurityManager.checkedRun(restricted, compiledCode.eval()).toString
      }(executionContext).awaitResult(timeout)
    } finally {
      System.setSecurityManager(null)
    }
    result
  }
}
