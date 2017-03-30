package com.lynxanalytics.biggraph

import java.security.{ AccessControlContext, AccessController, Permission }
import javax.script._

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain

class ScalaScriptSecurityManager extends SecurityManager {
  // These are temporary debug tools
  private val logs = scala.collection.mutable.Queue[String]()
  private def makeLog(entries: String*): Unit = {
    val head = Seq[String](Thread.currentThread.getName, Thread.currentThread.getId.toString)
    val entry = (head ++ entries).mkString(" ")
    logs += entry
  }
  def dump(pattern: String = ""): Unit = {
    logs.filter(_.contains(pattern)).foreach(println)
  }

  private val ImpossibleID: Long = -1L
  private var restrictedThreadID: Long = ImpossibleID

  private def inRestrictedThread = {
    restrictedThreadID == Thread.currentThread.getId
  }

  //  private def logWrapper[T](f: => T): T = {
  //    val ret = try {
  //      f
  //    } catch {
  //      case x: Throwable =>
  //        makeLog("THROWS: ", x.getMessage)
  //        throw x
  //    }
  //    ret
  //  }

  // This can only be done once during the lifetime of
  // this object
  def disableCurrentThread(): Unit = synchronized {
    if (restrictedThreadID != ImpossibleID) {
      throw new java.security.AccessControlException(s"RestrictedThreadID access")
    }
    restrictedThreadID = Thread.currentThread.getId
  }

  override def checkCreateClassLoader(): Unit = synchronized {
    makeLog("checkCreateClassLoader")
  }

  // Restricted threads cannot create other threads
  override def getThreadGroup: ThreadGroup = synchronized {
    makeLog("getThreadGroup called")
    if (inRestrictedThread) {
      throw new java.security.AccessControlException("Access error: GET_THREAD_GROUP")
    } else {
      super.getThreadGroup
    }
  }
  // Other permissions seem to be handled properly by the default SecurityManager
  override def checkPermission(permission: Permission): Unit = synchronized {
    makeLog("checkPermisson: ", permission.toString)
    try {
      if (inRestrictedThread &&
        !permission.toString.contains(".sbt")) {
        super.checkPermission(permission)
      }
    } catch {
      case x: Throwable =>
        makeLog("FAILED")
        throw x
    }
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

  def run(code: String, restricted: Boolean = false, dump: Boolean = false): String = {
    // IMAIN.compile changes the class loader and does not restore it.
    // https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    println(cl)
    val result = try {
      run_inner(code, restricted, dump, cl)
    } finally {
      Thread.currentThread().setContextClassLoader(cl)
    }
    result
  }

  def run_inner(code: String, restricted: Boolean, dump: Boolean, cl: ClassLoader): String = {
    val sm = new ScalaScriptSecurityManager
    assert(System.getSecurityManager == null)
    System.setSecurityManager(sm)
    val result = try {
      val compiledCode = engine.compile(code)
      SafeFuture {
        val cl = Thread.currentThread().getContextClassLoader
        println(cl)
        Thread.currentThread().setContextClassLoader(cl)
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
