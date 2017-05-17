// A very lazy collection for accessing parameter values and metadata in frontend operations.
//
// Example:
//
//   val params =
//     LazyParameters(context) +
//       Param("one", "One") + Param("two", params("one")) ++
//         (if (params("two").nonEmpty) List(Param("three", "Three")) else Nil)
//
// Constructing a parameter list is nearly identical to building a normal Scala List.
// "+" can be used to add single elements and "++" can be used to add sequences.
// But both "+" and "++" evaluate their parameter lazily. When the example code runs, none of the
// "Param()" constructors get executed. They will only get executed when a parameter is accessed.
//
// This makes it possible for one parameter to depend on the value of another parameter. In the
// example the value of "one" is used as the display name of "two", and "three" is only added if
// "two" is non-empty.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import scala.util._

class ParameterHolder(context: Operation.Context) {
  val metas = collection.mutable.Buffer[OperationParameterMeta]()
  val metaMap = collection.mutable.Map[String, OperationParameterMeta]()
  val errors = collection.mutable.Buffer[Throwable]()

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
    val extraIds = keys &~ paramIds
    assert(extraIds.size == 0,
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
}
