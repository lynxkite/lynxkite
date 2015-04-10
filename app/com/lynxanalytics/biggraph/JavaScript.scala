// A convenient interface for evaluating JavaScript expressions.
package com.lynxanalytics.biggraph

import scala.util.{ Failure, Success, Try }

case class JavaScript(expression: String) {
  def isEmpty = expression.isEmpty
  def nonEmpty = expression.nonEmpty

  def isTrue(mapping: (String, String)*): Boolean = isTrue(mapping.toMap)
  def isTrue(mapping: Map[String, String]): Boolean = {
    if (isEmpty) {
      return true
    }
    return evaluate(mapping) match {
      case result: java.lang.Boolean =>
        result
      case result =>
        throw JavaScript.Error(s"JS expression ($expression) returned $result instead of a Boolean")
    }
  }

  def evaluate(mapping: Map[String, Any]): Any = {
    val bindings = JavaScript.engine.createBindings
    for ((key, value) <- mapping) {
      bindings.put(key, value)
    }
    return Try(JavaScript.engine.eval(expression, bindings)) match {
      case Success(result) =>
        result
      case Failure(e) =>
        throw JavaScript.Error(s"Could not evaluate JS: $expression", e)
    }
  }
}
object JavaScript {
  val engine = new javax.script.ScriptEngineManager().getEngineByName("JavaScript")
  case class Error(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}

