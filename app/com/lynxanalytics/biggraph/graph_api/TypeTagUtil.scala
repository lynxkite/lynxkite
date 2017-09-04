// Some shared code for manipulating TypeTags.
package com.lynxanalytics.biggraph.graph_api

import scala.reflect.runtime.universe._
import scala.language.higherKinds

object TypeTagUtil {
  // Returns TypeTags for the type parameters of T.
  // For example typeArgs(typeTag[Map[Int, Double]]) returns Seq(typeTag[Int], typeTag[Double]).
  def typeArgs(tt: TypeTag[_]): Seq[TypeTag[_]] = {
    val args = tt.tpe.asInstanceOf[TypeRefApi].args
    val mirror = tt.mirror
    args.map { arg =>
      TypeTag(mirror, new reflect.api.TypeCreator {
        def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
          assert(m eq mirror, s"TypeTag[$arg] defined in $mirror cannot be migrated to mirror $m.")
          arg.asInstanceOf[U#Type]
        }
      })
    }
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
