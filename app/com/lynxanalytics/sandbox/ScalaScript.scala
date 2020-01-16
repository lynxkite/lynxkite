package com.lynxanalytics.sandbox

import java.io.FilePermission
import java.net.NetPermission
import java.security.Permission
import java.util.UUID
import javax.script._

import com.lynxanalytics.biggraph.graph_api.SafeFuture
import com.lynxanalytics.biggraph.graph_api.ThreadUtil
import com.lynxanalytics.biggraph.graph_api.TypeTagUtil
import com.lynxanalytics.biggraph.graph_util.{ LoggedEnvironment, Timestamp }
import com.lynxanalytics.biggraph.graph_util.SoftHashMap
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._
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
          case _: java.lang.reflect.ReflectPermission =>
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
        s.contains("org.apache.spark"))) {
      throw new java.security.AccessControlException(s"Illegal package access: $s")
    }
  }
}

object ScalaScript {
  private def createEngine(): IMain = {
    val e = new ScriptEngineManager().getEngineByName("scala").asInstanceOf[IMain]
    e.settings.usejavacp.value = true
    e.settings.embeddedDefaults[ScalaScriptSecurityManager]
    e
  }

  private var engine: IMain = null

  private val runCache = new SoftHashMap[String, String]()
  def run(
    code: String,
    bindings: Map[String, String] = Map(),
    extraParameters: String = "",
    timeoutInSeconds: Long = 10L): String = {
    import org.apache.commons.lang.StringEscapeUtils
    val binds = bindings.map {
      case (k, v) => s"""val $k: String = "${StringEscapeUtils.escapeJava(v)}" """
    }.mkString("\n")
    val fullCode = s"""
    $binds
    $extraParameters
    val result = {
      $code
    }.toString
    result
    """
    runCache.syncGetOrElseUpdate(fullCode, synchronized {
      withContextClassLoader {
        val compiledCode = compile(fullCode)
        withTimeout(timeoutInSeconds) {
          ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
            compiledCode.eval().toString
          }
        }
      }
    })
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
      val plot = {
        $code
      }
      plot.toJson.toString
      """
      val compiledCode = compile(fullCode)
      withTimeout(timeoutInSeconds) {
        ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
          compiledCode.eval().toString
        }
      }
    }
  }

  // A wrapper class representing the type signature of a Scala expression.
  import scala.language.existentials
  case class ScalaType(funcType: TypeTag[_]) {
    val returnType = funcType.tpe.typeArgs.last
    // Whether the expression returns an Option or not.
    def isOptionType = TypeTagUtil.isSubtypeOf[Option[_]](returnType)
    // The type argument for Options otherwise the return type.
    def payloadType = if (isOptionType) {
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
    codeReturnTypeCache.syncGetOrElseUpdate(id, ScalaType(inferType(funcCode)))
  }

  private def inferType(func: String): TypeTag[_] = synchronized {
    val fullCode = s"""
    import scala.reflect.runtime.universe._
    def typeTagOf[T: TypeTag](t: T) = typeTag[T]
    $func
    typeTagOf(eval _)
    """
    withContextClassLoader {
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

  private class CompileReturnedNullError() extends RuntimeException
  // Compiles the fullCode using the engine, and throws a ScriptException with a meaningful
  // error message in case of a compilation error.
  private def compile(fullCode: String) = {
    // The Scala compiler doesn't include the compilation error message, but prints it to the
    // console, so we need to capture the console.
    val os = new java.io.ByteArrayOutputStream
    try {
      Console.withOut(os) {
        val script = engine.compile(fullCode)
        // See #7227
        // We re-create the engine if engine.compile() returns a null
        if (script == null) {
          engine = null
          throw new CompileReturnedNullError()
        }
        script
      }
    } catch {
      case _: javax.script.ScriptException =>
        throw new javax.script.ScriptException(new String(os.toByteArray(), "UTF-8"))
      case _: CompileReturnedNullError =>
        throw new javax.script.ScriptException("Compile error")
    }
  }

  // A wrapper class to call the compiled function with the parameter Map.
  case class Evaluator(evalFunc: Function1[Map[String, Any], AnyRef]) {
    def evaluate(params: Map[String, Any]): AnyRef = {
      ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
        evalFunc.apply(params)
      }
    }
  }

  def compileAndGetEvaluator(
    code: String,
    mandatoryParamTypes: Map[String, TypeTag[_]],
    optionalParamTypes: Map[String, TypeTag[_]] = Map()): Evaluator = synchronized {
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
    withContextClassLoader {
      val compiledCode = compile(fullCode)
      val result = ScalaScriptSecurityManager.restrictedSecurityManager.checkedRun {
        compiledCode.eval()
      }
      Evaluator(result.asInstanceOf[Function1[Map[String, Any], AnyRef]])
    }
  }

  private def withContextClassLoader[T](func: => T): T = synchronized {
    // IMAIN.compile changes the class loader and does not restore it.
    // https://issues.scala-lang.org/browse/SI-8521
    if (engine == null) engine = createEngine()
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
      SafeFuture.async(func)(executionContext).awaitResult(Duration(timeoutInSeconds, "second"))
    } finally {
      executionContext.shutdownNow()
    }
  }

  def findVariables(code: String): Set[String] = {
    import scala.reflect.internal.util.ScriptSourceFile
    import scala.reflect.internal.util.NoFile
    withContextClassLoader {
      val script = ScriptSourceFile(NoFile, code.toArray)
      val global = engine.global
      val ast = new global.syntaxAnalyzer.SourceFileParser(script).parse()
      ast.collect({ case tree: global.syntaxAnalyzer.global.Ident => tree })
        .filter(i => i.isTerm)
        .map { case i => if (i.isBackquoted) i.name.decodedName.toString else i.toString }
        .toSet
    }
  }
}
