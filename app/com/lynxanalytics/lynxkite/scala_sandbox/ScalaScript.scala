package com.lynxanalytics.lynxkite.scala_sandbox

import java.io.FilePermission
import java.net.NetPermission
import java.security.Permission
import java.util.UUID
import javax.script._

import com.lynxanalytics.lynxkite.Environment
import com.lynxanalytics.lynxkite.graph_api.SafeFuture
import com.lynxanalytics.lynxkite.graph_api.ThreadUtil
import com.lynxanalytics.lynxkite.graph_api.TypeTagUtil
import com.lynxanalytics.lynxkite.graph_util.SoftHashMap
import com.lynxanalytics.lynxkite.spark_util.SQLHelper
import com.lynxanalytics.lynxkite.graph_util.Timestamp
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._
import scala.tools.nsc.interpreter.Scripted
import scala.util.DynamicVariable

// For describing project structure in parametric parameters.
// This needs to be in this package for the sandboxed scripts to access it.
case class SimpleGraphEntity(name: String, typeName: String)

object ScalaScriptSecurityManager {
  private[scala_sandbox] val restrictedSecurityManager = new ScalaScriptSecurityManager
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
          case _: java.lang.reflect.ReflectPermission =>
          case p: java.lang.RuntimePermission =>
            if (
              !(p.getName == "accessDeclaredMembers"
                || p.getName == "setContextClassLoader"
                || p.getName == "createClassLoader")
            ) {
              super.checkPermission(p)
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
    if (
      shouldCheck.value &&
      (s.contains("com.lynxanalytics.lynxkite") ||
        s.contains("org.apache.spark"))
    ) {
      throw new java.security.AccessControlException(s"Illegal package access: $s")
    }
  }
}

object ScalaScript {
  private def createEngine(): Scripted = {
    val settings = new scala.tools.nsc.Settings
    // When running with spark-submit, the classpath is made up of two parts: 1) all the jars
    // from Spark and 2) the LynxKite jar. The Spark jars are on the normal Java classpath, but
    // the LynxKite jar is added by Spark at runtime in the last ClassLoader. For Scala to find
    // our classes, we add our jar to the user-specified classpath.
    val cl = classOf[ScalaScriptSecurityManager].getClassLoader
    cl match {
      case cl: java.net.URLClassLoader if cl.getURLs.length == 1 =>
        val jar = cl.getURLs.toList.head.toString
        settings.classpath.value = jar
      case _ => ()
    }
    scala.tools.nsc.interpreter.Scripted(settings = settings)
  }

  private var engine: Scripted = null

  private val evaluatorCache = new SoftHashMap[String, Evaluator]()
  def run(
      code: String,
      bindings: Map[String, String] = Map()): String = {
    val e = compileAndGetEvaluator(code, bindings.keys.map(k => k -> typeTag[String]).toMap)
    e.evaluate(bindings).toString
  }

  // Helper function to convert a DataFrame to a Seq of Maps
  // This format used by the Vegas plot drawing library
  // The value of maxRows is the maximum number of data points allowed in a chart
  def dfToSeq(df: DataFrame): Seq[Map[String, Any]] = {
    val names = df.schema.toList.map { field => field.name }
    val maxRows = Environment.envOrElse("MAX_ROWS_OF_PLOT_DATA", "10000").toInt
    SQLHelper.toSeqRDD(df).take(maxRows).map {
      row =>
        names.zip(row.toSeq.toList).groupBy(_._1).mapValues(_.map(_._2)).mapValues(_(0))
    }.toSeq
  }

  // A wrapper class representing the type signature of a Scala expression.
  import scala.language.existentials
  case class ScalaType(funcType: TypeTag[_]) {
    val returnType = funcType.tpe.typeArgs.last
    // Whether the expression returns an Option or not.
    def isOptionType = TypeTagUtil.isSubtypeOf[Option[_]](returnType)
    // The type argument for Options otherwise the return type.
    def payloadType =
      if (isOptionType) {
        returnType.typeArgs(0)
      } else {
        returnType
      }
  }

  // Inferring the type for the exact same code and parameters is expected to happen often,
  // so it is better to use a cache.
  private val codeReturnTypeCache = new SoftHashMap[UUID, ScalaType]()

  def compileAndGetType(
      code: String,
      mandatoryParamTypes: Map[String, TypeTag[_]],
      optionalParamTypes: Map[String, TypeTag[_]] = Map()): ScalaType = {
    val funcCode = evalFuncString(
      code,
      mandatoryParamTypes ++ optionalParamTypes.mapValues { case v => TypeTagUtil.optionTypeTag(v) })
    val id = UUID.nameUUIDFromBytes(funcCode.getBytes())
    codeReturnTypeCache.getOrElseUpdate(id, ScalaType(inferType(funcCode)))
  }

  private def inferType(func: String): TypeTag[_] = synchronized {
    val fullCode = s"""
    import scala.reflect.runtime.universe._
    def typeTagOf[T: TypeTag](t: T) = typeTag[T]
    $func
    typeTagOf(eval _)
    """
    withEngine {
      val compiledCode = compile(fullCode)
      val result = ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
        compiledCode.eval()
      }
      result.asInstanceOf[TypeTag[_]] // We cannot use asInstanceOf within the SecurityManager.
    }
  }

  // Both type inference and evaluation should use this function.
  private def evalFuncString(
      code: String,
      convertedParamTypes: Map[String, TypeTag[_]]): String = {
    val paramString = convertedParamTypes.map {
      case (k, v) => s"`$k`: ${v.tpe}"
    }.mkString(", ")
    s"""
    def eval($paramString) = {
      $code
    }
    """
  }

  // Compiles the code and makes error messages prettier.
  private def compile(code: String) = {
    try {
      engine.compile("// CODE START\n" + code)
    } catch {
      case e: ScriptException =>
        // The exception includes the whole code (including internal details)
        // and does not highlight the line with the error. We reformat it here.
        val msg = e.getMessage
        val withoutCode = msg.replaceAll("(?s)\\s*// CODE START.*", "")
        val codeLines = e.getFileName.split("\n", -1)
        throw new Exception(Seq(
          withoutCode,
          codeLines(e.getLineNumber - 11),
          " " * (e.getColumnNumber - 1) + "^\n").mkString("\n"))
    }
  }

  // A wrapper class to call the compiled function with the parameter Map.
  case class Evaluator(code: String, evalFunc: Function1[Map[String, Any], AnyRef]) {
    def evaluate(params: Map[String, Any]): AnyRef = {
      try {
        ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
          evalFunc.apply(params)
        }
      } catch {
        case t: Throwable =>
          throw new Exception(s"Error while executing: $code", t)
      }
    }
  }

  def compileAndGetEvaluator(
      code: String,
      mandatoryParamTypes: Map[String, TypeTag[_]],
      optionalParamTypes: Map[String, TypeTag[_]] = Map()): Evaluator = {
    val cacheKey = (
      Seq(code) ++
        mandatoryParamTypes.toSeq.sortBy(_._1).map { x => s"${x._1}@${x._2.tpe}" }
        ++
        optionalParamTypes.toSeq.sortBy(_._1).map { x => s"${x._1}@${x._2.tpe}" })
      .mkString(";")
    evaluatorCache.getOrElseUpdate(
      cacheKey,
      synchronized {
        // Parameters are back quoted and taken out from the Map. The input argument is one Map to
        // make the calling of the compiled function easier (otherwise we had varying number of args).
        val convertedParamTypes = mandatoryParamTypes ++
          optionalParamTypes.mapValues { case v => TypeTagUtil.optionTypeTag(v) }
        val paramsString = convertedParamTypes.map {
          case (k, t) => s"""val `$k` = params("$k").asInstanceOf[${t.tpe}]"""
        }.mkString("\n")
        val callParams = convertedParamTypes.map {
          case (k, _) => s"`$k`"
        }.mkString(", ")
        val func = evalFuncString(code, convertedParamTypes)
        val fullCode = s"""
      $func
      def evalWrapper(params: Map[String, Any]) = {
        $paramsString
        eval($callParams)
      }
      evalWrapper _
      """
        withEngine {
          val compiledCode = compile(fullCode)
          val result = ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
            compiledCode.eval()
          }
          Evaluator(fullCode, result.asInstanceOf[Function1[Map[String, Any], AnyRef]])
        }
      },
    )
  }

  private def withEngine[T](func: => T): T = synchronized {
    engine = createEngine()
    try {
      func
    } finally {
      engine = null
    }
  }

  private def withTimeout[T](timeoutInSeconds: Long)(func: => T): T = {
    val ctxName = s"RestrictedScalaScript-${Timestamp.toString}"
    val executionContext = ThreadUtil.limitedExecutionContext(ctxName, 1)
    try {
      SafeFuture.async(func)(executionContext).awaitResult(Duration(timeoutInSeconds, "second"))
    } finally {
      executionContext.shutdownNow()
    }
  }

  private val findVariablesCache = new SoftHashMap[String, Set[String]]()

  def findVariables(code: String): Set[String] = {
    import scala.reflect.internal.util.ScriptSourceFile
    import scala.reflect.internal.util.NoFile
    findVariablesCache.getOrElseUpdate(
      code,
      withEngine {
        val script = ScriptSourceFile(NoFile, code.toArray)
        val global = engine.intp.global
        val ast = new global.syntaxAnalyzer.SourceFileParser(script).parse()
        ast.collect({ case tree: global.syntaxAnalyzer.global.Ident => tree })
          .filter(i => i.isTerm)
          .map { case i => if (i.isBackquoted) i.name.decodedName.toString else i.toString }
          .toSet
      },
    )
  }
}
