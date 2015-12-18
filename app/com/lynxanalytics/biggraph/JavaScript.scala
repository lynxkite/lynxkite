// A convenient interface for evaluating JavaScript expressions.
package com.lynxanalytics.biggraph

import scala.util.{ Failure, Success, Try }
import org.mozilla.javascript;

case class JavaScript(expression: String) {
  def isEmpty = expression.isEmpty
  def nonEmpty = expression.nonEmpty

  def isTrue(mapping: (String, String)*): Boolean = isTrue(mapping.toMap)
  def isTrue(mapping: Map[String, String]): Boolean = {
    if (isEmpty) {
      return true
    }
    return evaluate(mapping, classOf[Boolean]).asInstanceOf[Boolean]
  }

  def evaluate(mapping: Map[String, Any], desiredClass: java.lang.Class[_]): AnyRef = {
    val cx = javascript.Context.enter()
    try {
      val scope = cx.initSafeStandardObjects()
      mapping.foreach {
        case (name, value) =>
          val jsValue = javascript.Context.javaToJS(value, scope)
          javascript.ScriptableObject.putProperty(scope, name, jsValue)
      }
      javascript.ScriptableObject.putProperty(scope, "util", JavaScriptUtilities)
      val jsResult = cx.evaluateString(scope, expression, "derivation script", 1, null)
      jsResult match {
        case _: javascript.Undefined => null
        case definedValue => javascript.Context.jsToJava(definedValue, desiredClass)
      }
    } finally {
      javascript.Context.exit()
    }
  }
}
object JavaScript {
  val engine = new javax.script.ScriptEngineManager().getEngineByName("JavaScript")
  case class Error(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}

// This object is exposed to user-authored JavaScript. Only harmless stuff, please.
object JavaScriptUtilities {
  def rnd(seed: Int) = new scala.util.Random(seed)
  def hash(str: String) = str.hashCode
}
