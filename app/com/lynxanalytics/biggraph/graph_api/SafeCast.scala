// Provides extra run-time type safety for graph attributes.
//
// Usage:
//
//   def asStringAttribute(attr: Attribute[_]): Attribute[String] = {
//     // Wrong solution, does not catch type mismatches:
//     attr.asInstanceOf[Attribute[String]]
//     // Good solution, fails if the parameter is not a String attribute:
//     attr.runtimeSafeCast[String]
//   }

package com.lynxanalytics.biggraph.graph_api

import scala.language.higherKinds
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object RuntimeSafeCastable {
  def classTagFromTypeTag[T: TypeTag]: ClassTag[T] = {
    val tt = typeTag[T]
    ClassTag[T](tt.mirror.runtimeClass(tt.tpe))
  }
}
trait RuntimeSafeCastable[T, ConcreteKind[T] <: RuntimeSafeCastable[T, ConcreteKind]] {
  implicit def typeTag: TypeTag[T]

  def runtimeSafeCast[S: TypeTag]: ConcreteKind[S] = {
    if (typeOf[S] =:= typeOf[T]) {
      this.asInstanceOf[ConcreteKind[S]]
    } else throw new ClassCastException("Cannot cast from %s to %s".format(typeOf[T], typeOf[S]))
  }

  def classTag = RuntimeSafeCastable.classTagFromTypeTag[T]

  def is[S: TypeTag]: Boolean = {
    typeOf[S] =:= typeOf[T]
  }
}
