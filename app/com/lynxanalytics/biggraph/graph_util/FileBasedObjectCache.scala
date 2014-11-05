package com.lynxanalytics.biggraph.graph_util

import scala.collection.mutable.HashMap
import scala.ref.WeakReference

object FileBasedObjectCache {
  private val cache = HashMap[Filename, WeakReference[AnyRef]]()
  def get[T](filename: Filename): T = synchronized {
    cache.get(filename).flatMap(_.get).getOrElse {
      val value = filename.loadObjectKryo.asInstanceOf[AnyRef]
      cache(filename) = WeakReference(value)
      value
    }.asInstanceOf[T]
  }
}
