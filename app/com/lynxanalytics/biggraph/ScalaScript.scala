package com.lynxanalytics.biggraph

import java.security.{ AccessControlContext, AccessController, Permission }
import javax.script._

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain

class ScalaScriptSecurityManager extends SecurityManager {
  val logs = scala.collection.mutable.Queue[(String, Long, Long)]()
  var restrictedThreadID: Long = -1L

  override def getThreadGroup: ThreadGroup = synchronized {
    val name = Thread.currentThread.getName + "_" + " GET_THREAD_GROUP"
    val q = (name, restrictedThreadID, Thread.currentThread().getId)
    logs += q
    if (Thread.currentThread().getId == restrictedThreadID) {
      throw new java.security.AccessControlException("Access error: GET_THREAD_GROUP")
    } else {
      super.getThreadGroup
    }
  }
  override def checkPermission(permission: Permission): Unit = synchronized {
    val l = (Thread.currentThread().getName + "_" + permission.toString, restrictedThreadID, Thread.currentThread().getId)
    logs += l
    if (Thread.currentThread().getId == restrictedThreadID) {
      super.checkPermission(permission)
      //     val q = ("ERROR" + Thread.currentThread().getName + "_" + permission.toString, restrictedThreadID, Thread.currentThread().getId)
      //      logs += q
      //      Thread.dumpStack()
      //      throw new java.security.AccessControlException(s"Access error: $permission")
    }
  }
  def disableCurrentThread(): Unit = synchronized {
    if (restrictedThreadID != -1L) {
      throw new java.security.AccessControlException(s"RestrictedThreadID access")
    }
    restrictedThreadID = Thread.currentThread().getId
  }
  def dumpLogs(filter: String = ""): Unit = synchronized {
    for (v <- logs) {
      if (v._1 contains (filter)) {
        println(v)
      }
    }
  }
}

object ScalaScript {
  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
  private val timeout = Duration(5L, "second")
  private val executionContext = ThreadUtil.limitedExecutionContext("RestrictedScalaScript", 1)

  private val settings = engine.settings
  settings.usejavacp.value = true

  def run(code: String): String = {
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    val sm = new ScalaScriptSecurityManager
    assert(System.getSecurityManager == null)
    System.setSecurityManager(sm)
    val result = try {
      val compiledCode = engine.compile(code)
      SafeFuture {
        sm.disableCurrentThread()
        compiledCode.eval().toString
      }(executionContext).awaitResult(timeout)
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
      System.setSecurityManager(null)
      sm.dumpLogs("")
    }
    result
  }
}
