package com.lynxanalytics.second

import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }
import java.lang.Thread.currentThread
import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain

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

  private var restrictedThread: java.lang.Thread = null
  // This can only be done once during the lifetime of
  // this object.
  // Once this is done, the Thread cannot be garbage collected
  // as long as this security manager is not garbage collected
  // because of the reference.
  def disableCurrentThread(): Unit = synchronized {
    log("DISABLED")
    if (restrictedThread != null) {
      throw new java.security.AccessControlException(s"RestrictedThreadID access")
    }
    restrictedThread = Thread.currentThread
  }

  private def inRestrictedThread = synchronized {
    (restrictedThread) != null && (restrictedThread eq currentThread)
  }

  // Restricted threads cannot create other threads
  override def getThreadGroup: ThreadGroup = {
    log("getThreadGroup called")
    if (inRestrictedThread) {
      throw new java.security.AccessControlException("Access error: GET_THREAD_GROUP")
    } else {
      super.getThreadGroup
    }
  }
  // Other permissions seem to be handled properly by the default SecurityManager
  override def checkPermission(permission: Permission): Unit = {
    log("checkPermisson: ", permission.toString)
    if (inRestrictedThread && !permission.toString.contains(".sbt")) {
      super.checkPermission(permission)
    }
  }

  override def checkPackageAccess(s: String): Unit = {
    super.checkPackageAccess(s)
    log("checkingPackageAccess: ", s)
    if (inRestrictedThread) {
      if (s.startsWith("com.lynxanalytics")) {
        throw new java.security.AccessControlException("Illegal package access")
      }
    }
  }

  override def checkPackageDefinition(s: String): Unit = {
    log("checkingPackageDefinition: ", s)
    super.checkPackageDefinition(s)
  }

}

object ScalaScript {
  private val engine = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
  private val timeout = Duration(10L, "second")
  // There can only be one restricted thread
  private val executionContext = ThreadUtil.limitedExecutionContext("RestrictedScalaScript", 1)

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
        if (restricted) {
          sm.disableCurrentThread()
        }
        compiledCode.eval().toString
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
