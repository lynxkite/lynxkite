package com.lynxanalytics.biggraph.graph_api

import scala.language.higherKinds
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

trait RuntimeSafeCastable[T, ConcreteKind[T] <: RuntimeSafeCastable[T, ConcreteKind]] {
  implicit def typeTag: TypeTag[T]

  def runtimeSafeCast[S: TypeTag]: ConcreteKind[S] = {
    if (typeOf[S] =:= typeOf[T]) {
      this.asInstanceOf[ConcreteKind[S]]
    } else throw new ClassCastException("Cannot cast from %s to %s".format(typeOf[T], typeOf[S]))
  }

  def classTag: ClassTag[T] = {
    ClassTag[T](typeTag.mirror.runtimeClass(typeTag.tpe))
  }
  def is[S: TypeTag]: Boolean = {
    typeOf[S] =:= typeOf[T]
  }
}

