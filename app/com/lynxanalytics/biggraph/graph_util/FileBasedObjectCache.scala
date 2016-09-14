// Loads and caches objects from files. It is used to share data between the
// workers through a distributed file system.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object FileBasedObjectCache {
  private val cache = new SoftHashMap[HadoopFile, AnyRef]()
  def get[T](filename: HadoopFile): T = synchronized {
    cache.getOrElseUpdate(filename, {
      log.info(s"Loading object from $filename...")
      val value = filename.loadObjectKryo.asInstanceOf[AnyRef]
      log.info(s"Loaded object from $filename.")
      value
    }).asInstanceOf[T]
  }
}
