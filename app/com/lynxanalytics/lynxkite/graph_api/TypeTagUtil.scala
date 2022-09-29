// Some shared code for manipulating TypeTags.
package com.lynxanalytics.lynxkite.graph_api

import scala.reflect.runtime.universe._
import scala.language.higherKinds
import scala.annotation.meta.field

class SubTypeCreator(tt: TypeTag[_], i: Int)
    extends reflect.api.TypeCreator {
  def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
    assert(
      m eq tt.mirror,
      s"TypeTag[$tt] defined in ${tt.mirror} cannot be migrated to mirror $m.")
    val args = tt.tpe.asInstanceOf[TypeRefApi].args
    args(i).asInstanceOf[U#Type]
  }
  def typeTag = TypeTag(tt.mirror, this)
}

object TypeTagUtil {
  // Returns TypeTags for the type parameters of T.
  // For example typeArgs(typeTag[Map[Int, Double]]) returns Seq(typeTag[Int], typeTag[Double]).
  def typeArgs(tt: TypeTag[_]): Seq[TypeTag[_]] = {
    val args = tt.tpe.asInstanceOf[TypeRefApi].args
    args.indices.map(i => new SubTypeCreator(tt, i).typeTag)
  }

  def optionTypeTag[T: TypeTag] = typeTag[Option[T]]
  def arrayTypeTag[T: TypeTag] = typeTag[Array[T]]
  def vectorTypeTag[T: TypeTag] = typeTag[Vector[T]]
  def tuple2TypeTag[T1: TypeTag, T2: TypeTag] = typeTag[Tuple2[T1, T2]]
  // Call mapTypeTag with explicit parameters to make sure the key and value are not switched.
  def mapTypeTag[K, V](implicit kt: TypeTag[K], vt: TypeTag[V]) = typeTag[Map[K, V]]
  def setTypeTag[T: TypeTag] = typeTag[Set[T]]
  def isType[T: TypeTag](t: Type) = t =:= typeOf[T]
  def isSubtypeOf[T: TypeTag](t: Type) = t <:< typeOf[T]
  def isOfKind1[T[_]](t: Type)(implicit tt: TypeTag[T[Any]]) =
    t.typeConstructor =:= typeOf[T[Any]].typeConstructor
  def isOfKind2[T[_, _]](t: Type)(implicit tt: TypeTag[T[Any, Any]]) =
    t.typeConstructor =:= typeOf[T[Any, Any]].typeConstructor
}
