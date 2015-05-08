// Loads and caches objects from files. It is used to share data between the
// workers through a distributed file system.
package com.lynxanalytics.biggraph.graph_util

import scala.collection.mutable.HashMap
import scala.ref.SoftReference
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object FileBasedObjectCache {
  private val cache = HashMap[HadoopFile, SoftReference[AnyRef]]()
  def get[T](filename: HadoopFile): T = synchronized {
    cache.get(filename).flatMap(_.get).getOrElse {
      log.info(s"Loading object from $filename...")
      val value = filename.loadObjectKryo.asInstanceOf[AnyRef]
      log.info(s"Loaded object from $filename.")
      cache(filename) = new SoftReference(value)
      value
    }.asInstanceOf[T]
  }
}
