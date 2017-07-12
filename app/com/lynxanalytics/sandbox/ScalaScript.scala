package com.lynxanalytics.sandbox

import java.io.FilePermission
import java.net.NetPermission
import java.security.Permission
import javax.script._

import com.lynxanalytics.biggraph.graph_api.SafeFuture
import com.lynxanalytics.biggraph.graph_api.ThreadUtil
import com.lynxanalytics.biggraph.graph_util.{ LoggedEnvironment, Timestamp }
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.IMain
import scala.util.DynamicVariable

object ScalaScriptSecurityManager {
  private[sandbox] val restrictedSecurityManager = new ScalaScriptSecurityManager
  System.setSecurityManager(restrictedSecurityManager)
  def init() = {}
}

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

  private def calledByClassLoader: Boolean = {
    Thread.currentThread.getStackTrace.exists {
      stackTraceElement =>
        stackTraceElement.getClassName == "java.lang.ClassLoader" &&
          stackTraceElement.getMethodName == "loadClass"
    }
  }

  override def checkPermission(permission: Permission): Unit = {
    if (shouldCheck.value) {
      shouldCheck.value = false
      try {
        permission match {
          case _: NetPermission =>
            if (!calledByClassLoader) {
              super.checkPermission(permission)
            }
          case p: FilePermission =>
            // File reads are allowed if they are initiated by the class loader.
            if (!(p.getActions == "read" && calledByClassLoader)) {
              super.checkPermission(permission)
            }
          case _ =>
            super.checkPermission(permission)
        }
      } finally {
        // "finally" is used instead of "withValue" because "withValue" triggers
        // class loading for the anonymous class and thus infinite recursion.
        shouldCheck.value = true
      }
    }
  }

  override def checkPermission(permission: Permission, o: scala.Any): Unit = {
    if (shouldCheck.value) {
      super.checkPermission(permission, o)
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

  private val settings = engine.settings
  settings.usejavacp.value = true
  settings.embeddedDefaults[ScalaScriptSecurityManager]

  def run(
    code: String,
    bindings: Map[String, String] = Map(),
    timeoutInSeconds: Long = 10L): String = synchronized {
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
        ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
          compiledCode.eval().toString
        }
      }
    }
  }

  // Helper function to convert a DataFrame to a Seq of Maps
  // This format used by the Vegas plot drawing library
  // The value of maxRows is the maximum number of data points allowed in a chart
  def dfToSeq(df: DataFrame): Seq[Map[String, Any]] = {
    val names = df.schema.toList.map { field => field.name }
    val maxRows = LoggedEnvironment.envOrElse("MAX_ROWS_OF_PLOT_DATA", "10000").toInt
    SQLHelper.toSeqRDD(df).take(maxRows).map {
      row =>
        names.zip(row.toSeq.toList).
          groupBy(_._1).
          mapValues(_.map(_._2)).
          mapValues(_(0))
    }.toSeq
  }

  def runVegas(
    code: String,
    df: DataFrame): String = synchronized {
    // To avoid the need of spark packages in the script
    // we convert the DataFrame before passing it to Vegas
    val data = dfToSeq(df)
    val timeoutInSeconds = LoggedEnvironment.envOrElse("SCALASCRIPT_TIMEOUT_SECONDS", "10").toLong
    withContextClassLoader {
      engine.put("table: Seq[Map[String, Any]]", data)
      val fullCode = s"""
      import vegas._
      import vegas.spec.Spec.Scala
      val plot = {
        $code
      }
      plot.toJson.toString
      """
      val compiledCode = engine.compile(fullCode)
      withTimeout(timeoutInSeconds) {
        ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
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
