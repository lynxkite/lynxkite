// Frontend operations use ParameterHolder to define and access their parameters.
//
// Parameter has mutable state to which you can add one parameter with "+=" or multiple parameters
// with "++=". Parameters can refer to earlier defined parameters. For example:
//
//   params += Param("one", "One")
//   params += Param("two", params("one"))
//   params ++= (if (params("two").nonEmpty) List(Param("three", "Three")) else Nil)
//
// "+=" and "++=" handle exceptions in the definitions and save them. If the parameter is later
// accessed, the exception will surface. But the exceptions will not break the entire box. The
// successfully defined parameters can still be accessed from the frontend.
//
// Make sure the exception happens after "+=" / "++=". For example this variation of the third
// example will break the box if "two" has an error:
//
//   if (params("two").nonEmpty) params += Param("three", "Three")
//
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import play.api.libs.json.JsObject

import scala.util._

class ParameterHolder(context: Operation.Context) {
  private val metas = collection.mutable.Buffer[OperationParameterMeta]()
  private val metaMap = collection.mutable.Map[String, OperationParameterMeta]()
  private val errors = collection.mutable.Buffer[Throwable]()

  def apply(name: String): String = {
    if (context.box.parametricParameters.contains(name)) {
      com.lynxanalytics.sandbox.ScalaScript.run(
        "s\"\"\"" + context.box.parametricParameters(name) + "\"\"\"",
        context.workspaceParameters)
    } else if (context.box.parameters.contains(name)) {
      context.box.parameters(name)
    } else if (metaMap.contains(name)) {
      metaMap(name).defaultValue
    } else {
      assertNoErrors()
      throw new AssertionError(s"Undefined parameter: $name")
    }
  }

  def +=(fn: => OperationParameterMeta): Unit = {
    Try(fn) match {
      case Failure(e) =>
        errors += e
      case Success(meta) =>
        metas += meta
        metaMap(meta.id) = meta
    }
  }

  def ++=(more: => TraversableOnce[OperationParameterMeta]): Unit = {
    Try(more) match {
      case Failure(e) =>
        errors += e
      case Success(more) =>
        for (meta <- more) {
          metas += meta
          metaMap(meta.id) = meta
        }
    }
  }

  private def assertNoErrors(): Unit = {
    if (errors.nonEmpty) {
      val ae = new AssertionError("Error while defining parameters.")
      for (t <- errors) {
        ae.addSuppressed(t)
      }
      throw ae
    }
  }

  def validate(): Unit = {
    assertNoErrors()
    val dups = metas.groupBy(_.id).filter(_._2.size > 1).keys
    assert(dups.isEmpty, s"Duplicate parameter: ${dups.mkString(", ")}")
    val paramIds = metaMap.keySet
    val keys = context.box.parameters.keySet.union(context.box.parametricParameters.keySet)
    val extraIds = keys &~ paramIds // keys diff paramIds
    assert(extraIds.isEmpty,
      s"""Extra parameters found: ${extraIds.mkString(", ")} is not in ${paramIds.mkString(", ")}""")
    for (meta <- metas) {
      meta.validate(this(meta.id))
    }
  }

  def toFE: List[FEOperationParameterMeta] = metas.map(_.toFE).toList

  def toMap: Map[String, String] = {
    assertNoErrors()
    metas.map(m => m.id -> this(m.id)).toMap
  }

  def getMetaMap = metaMap.toMap
}
