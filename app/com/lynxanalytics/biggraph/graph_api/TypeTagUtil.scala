// Some shared code for manipulating TypeTags.
package com.lynxanalytics.biggraph.graph_api

import scala.reflect.runtime.universe._

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
}
