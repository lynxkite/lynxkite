package com.lynxanalytics.biggraph

import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain

class ScalaScriptSecurityManager(RestrictedThreadName: String) extends SecurityManager {
  override def checkPermission(permission: Permission): Unit = {
    if (Thread.currentThread().getName.startsWith(RestrictedThreadName)) {
      super.checkPermission(permission)
    }
  }
}

object ScalaScript {
  private val restrictedThreadName = "RestrictedScalaScript"
  assert(System.getSecurityManager == null)
  private val sm = new ScalaScriptSecurityManager(restrictedThreadName)
  System.setSecurityManager(sm)

  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
  private val timeout = Duration(5L, "second")
  private val executionContext = ThreadUtil.limitedExecutionContext(restrictedThreadName, 1)

  private val settings = engine.settings
  settings.usejavacp.value = true

  def run(code: String): String = {
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    val result = try {
      val compiledCode = engine.compile(code)
      SafeFuture {
        compiledCode.eval().toString
      }(executionContext).awaitResult(timeout)
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
    }
    result
  }
}
