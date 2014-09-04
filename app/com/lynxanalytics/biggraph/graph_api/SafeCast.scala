package com.lynxanalytics.biggraph.graph_api

import scala.language.higherKinds
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object RuntimeSafeCastable {
  def classTagFromTypeTag[T](tt: TypeTag[T]): ClassTag[T] =
    ClassTag[T](typeTag.mirror.runtimeClass(tt.tpe))
}
trait RuntimeSafeCastable[T, ConcreteKind[T] <: RuntimeSafeCastable[T, ConcreteKind]] {
  implicit def typeTag: TypeTag[T]

  def runtimeSafeCast[S: TypeTag]: ConcreteKind[S] = {
    if (typeOf[S] =:= typeOf[T]) {
      this.asInstanceOf[ConcreteKind[S]]
    } else throw new ClassCastException("Cannot cast from %s to %s".format(typeOf[T], typeOf[S]))
  }

  def classTag: ClassTag[T] = {
    RuntimeSafeCastable.classTagFromTypeTag(typeTag)
  }
  def is[S: TypeTag]: Boolean = {
    typeOf[T] =:= typeOf[S]
  }
}
