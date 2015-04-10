// Load and caches objects from files.
package com.lynxanalytics.biggraph.graph_util

import scala.collection.mutable.HashMap
import scala.ref.SoftReference
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object FileBasedObjectCache {
  private val cache = HashMap[Filename, SoftReference[AnyRef]]()
  def get[T](filename: Filename): T = synchronized {
    cache.get(filename).flatMap(_.get).getOrElse {
      log.info(s"Loading object from $filename...")
      val value = filename.loadObjectKryo.asInstanceOf[AnyRef]
      log.info(s"Loaded object from $filename.")
      cache(filename) = new SoftReference(value)
      value
    }.asInstanceOf[T]
  }
}
