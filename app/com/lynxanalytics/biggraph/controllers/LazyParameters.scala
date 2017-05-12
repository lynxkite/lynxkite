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

object LazyParameters {
  def apply(context: Operation.Context) = new LazyParameters(context, Stream())
}
class LazyParameters(context: Operation.Context, metas: Stream[() => OperationParameterMeta]) {
  def apply(name: String): String = {
    if (context.box.parametricParameters.contains(name)) {
      com.lynxanalytics.sandbox.ScalaScript.run(
        "s\"\"\"" + context.box.parametricParameters(name) + "\"\"\"",
        context.workspaceParameters)
    } else if (context.box.parameters.contains(name)) {
      context.box.parameters(name)
    } else goodMetas.find(_.id == name) match {
      case Some(meta) => meta.defaultValue
      case None =>
        logErrors()
        throw new AssertionError(s"Parameter not found: $name")
    }
  }

  def +(next: => OperationParameterMeta): LazyParameters = {
    new LazyParameters(context, metas :+ (() => next))
  }

  def ++(more: => Seq[OperationParameterMeta]): LazyParameters = {
    new LazyParameters(context, metas.append(more.map(m => () => m)))
  }

  private def allMetas: Seq[OperationParameterMeta] = {
    metas.map(fn => fn())
  }

  private val goodSoFar =
    new ThreadLocal[Seq[OperationParameterMeta]] { override def initialValue() = null }
  private def goodMetas: Seq[OperationParameterMeta] = {
    if (goodSoFar.get != null) {
      // This is a recursive call. Just return what we have so far.
      goodSoFar.get
    } else {
      // First call. Do the real work.
      goodSoFar.set(Seq())
      try {
        // This is where we call the functions that generate the parameters.
        // If a parameter depends on the value of another, fn() will call LazyParameters.apply
        // which may call goodMetas again. This is why we need the protection against recursion.
        for (fn <- metas) goodSoFar.set(goodSoFar.get :+ fn())
      } catch { case t: Throwable => }
      val good = goodSoFar.get
      goodSoFar.set(null)
      good
    }
  }

  private def logErrors(): Unit = {
    try {
      for (fn <- metas) fn()
    } catch {
      case t: Throwable => log.error("Failure while generating parameters.", t)
    }
  }

  def validate(): Unit = {
    val ms = allMetas
    val dups = ms.groupBy(_.id).filter(_._2.size > 1).keys
    assert(dups.isEmpty, s"Duplicate parameter: ${dups.mkString(", ")}")
    val paramIds = ms.map(_.id).toSet
    val keys = context.box.parameters.keySet.union(context.box.parametricParameters.keySet)
    val extraIds = keys &~ paramIds
    assert(extraIds.size == 0,
      s"""Extra parameters found: ${extraIds.mkString(", ")} is not in ${paramIds.mkString(", ")}""")
    for (meta <- ms) {
      meta.validate(this(meta.id))
    }
  }

  def toFE: List[FEOperationParameterMeta] = goodMetas.map(_.toFE).toList

  def toMap: Map[String, String] = allMetas.map(m => m.id -> this(m.id)).toMap
}
