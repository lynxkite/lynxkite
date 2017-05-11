package com.lynxanalytics.sandbox

import java.io.FilePermission
import java.net.NetPermission
import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.SafeFuture
import com.lynxanalytics.biggraph.graph_api.ThreadUtil
import com.lynxanalytics.biggraph.graph_util.Timestamp

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain
import scala.util.DynamicVariable

class ScalaScriptSecurityManager extends SecurityManager {

  val shouldCheck = new DynamicVariable[Boolean](false)
  def checkedRun[R](op: => R): R = {
    shouldCheck.withValue(true) {
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
          if (!(p.getActions == "read" && p.getName.contains(".sbt") && calledByClassLoader)) {
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
        s.contains("org.apache.spark") ||
        s.contains("scala.reflect"))) {
      throw new java.security.AccessControlException(s"Illegal package access: $s")
    }
  }
}

object ScalaScript {
  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]

  private val restrictedSecurityManager = new ScalaScriptSecurityManager
  System.setSecurityManager(restrictedSecurityManager)
  private val settings = engine.settings
  settings.usejavacp.value = true
  settings.embeddedDefaults[ScalaScriptSecurityManager]

  def run(
    code: String, bindings: Map[String, String] = Map(), timeoutInSeconds: Long = 10L): String = {
    import org.apache.commons.lang.StringEscapeUtils
    val binds = bindings.map {
      case (k, v) => s"""val $k: String = "${StringEscapeUtils.escapeJava(v)}" """
    }.mkString("\n")
    val fullCode = s"""
    $binds
    val result = {
      $code
    }.toString
    result
    """

    withContextClassLoader {
      val compiledCode = engine.compile(fullCode)
      withTimeout(timeoutInSeconds) {
        restrictedSecurityManager.checkedRun {
          compiledCode.eval().toString
        }
      }
    }
  }

  def runWithDouble( // this is a POC method
    code: String, x: Double, timeoutInSeconds: Long = 10L): String = synchronized {
    withContextClassLoader {
      engine.put("x: Double", x)
      val fullCode = s"""
      val result = {
          $code
      }.toString
      result
      """
      val compiledCode = engine.compile(fullCode)
      withTimeout(timeoutInSeconds) {
        restrictedSecurityManager.checkedRun {
          compiledCode.eval().toString
        }
      }
    }
  }

  def runVegas( // this is a POC method
    code: String, data: Seq[Map[String, Any]], title: String, timeoutInSeconds: Long = 10L): String = synchronized {
    withContextClassLoader {
      engine.put("dfData: Seq[Map[String, Any]]", data)
      engine.put("Vegas", vegas.Vegas)
      engine.put("Nominal", vegas.Nominal)
      engine.put("Quantitative", vegas.Quantitative)
      engine.put("Bar", vegas.Bar)

      val fullCode = s"""
      //import vegas._
      val result = {
        val plot = Vegas("$title").
        withData(dfData).
        $code
        val json: String = plot.toJson
        json
      }.toString
      result
      """
      println()
      print("**** Full code ****")
      print(fullCode)
      print("**** --------- ****")
      println()
      val compiledCode = engine.compile(fullCode)
      withTimeout(timeoutInSeconds) {
        restrictedSecurityManager.checkedRun {
          compiledCode.eval().toString
        }
      }
    }
  }

  private def withContextClassLoader[T](func: => T): T = {
    // IMAIN.compile changes the class loader and does not restore it.
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    try {
      func
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
    }
  }

  private def withTimeout[T](timeoutInSeconds: Long)(func: => T): T = {
    val ctxName = s"RestrictedScalaScript-${Timestamp.toString}"
    val executionContext = ThreadUtil.limitedExecutionContext(ctxName, 1)
    try {
      SafeFuture {
        func
      }(executionContext).awaitResult(Duration(timeoutInSeconds, "second"))
    } finally {
      executionContext.shutdownNow()
    }
  }
}
