package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json

object JsonMigration {
  type VersionMap = Map[String, Int]

  implicit val versionOrdering = new math.Ordering[VersionMap] {
    override def compare(a: VersionMap, b: VersionMap): Int = {
      val cmp = (a.keySet ++ b.keySet).map { k => a(k) compare b(k) }
      if (cmp.forall(_ == 0)) 0
      else if (cmp.forall(_ < 0)) -1
      else if (cmp.forall(_ > 0)) 1
      else {
        assert(false, s"Incomparable versions: $a, $b")
        ???
      }
    }
  }

  val current = new JsonMigration
}
import JsonMigration._
class JsonMigration {
  val version: VersionMap = Map().withDefaultValue(0)
  // Upgrader functions keyed by class name and starting version.
  // They take the JsObject from version X to version X + 1.
  val upgraders = Map[(String, Int), Function[json.JsObject, json.JsObject]]()
}
