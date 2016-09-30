package com.lynxanalytics.biggraph.graph_util

import scala.ref.SoftReference
import scala.collection.mutable.HashMap

class SoftHashMap[Key, Value <: AnyRef] {
  private val cache = HashMap[Key, SoftReference[Value]]()
  def getOrElseUpdate(key: Key, op: => Value): Value = {
    val option: Option[Value] = cache.get(key).flatMap(_.get)
    option match {
      case Some(v) => v
      case None => val v = op; cache(key) = new SoftReference(v); v
    }
  }
}
