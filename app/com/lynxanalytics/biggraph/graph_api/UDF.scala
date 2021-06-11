package com.lynxanalytics.biggraph.graph_api

import org.apache.spark.sql
import com.lynxanalytics.biggraph.graph_operations

object UDF {

  def hash(string: String, salt: String): String = graph_operations.HashVertexAttribute.hash(string, salt)

  val crs = org.geotools.referencing.CRS.decode("EPSG:4326")

  // Returns the geodetic distance between two lat/long coordinates in meters.
  def geodistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    import org.locationtech.jts.geom._
    import org.geotools.geometry.jts._
    import org.geotools.referencing._
    val gc = new GeodeticCalculator(UDF.crs)
    gc.setStartingPosition(JTS.toDirectPosition(new Coordinate(lat1, lon1), UDF.crs))
    gc.setDestinationPosition(JTS.toDirectPosition(new Coordinate(lat2, lon2), UDF.crs))
    gc.getOrthodromicDistance()
  }

  // Returns the intersection of the sets of Strings.
  def string_intersect(set1: Seq[String], set2: Seq[String]): Seq[String] = {
    for (v <- set1 if set2 contains v) yield v
  }

  /**
   * Custom aggregator for SQL queries.
   *
   * Computes the most frequent value in a String column.
   * Null values are not counted.
   */
  private type Counts = Map[String, Long]
  val mostCommon = new org.apache.spark.sql.expressions.Aggregator[String, Counts, String] {
    def zero = Map[String, Long]()
    def reduce(map: Counts, key: String): Counts =
      if (key == null) map
      else map + (key -> (map.getOrElse(key, 0L) + 1L))
    def merge(map1: Counts, map2: Counts): Counts =
      (map1.keySet ++ map2.keySet).toSeq.map(k => k -> (map1.getOrElse(k, 0L) + map2.getOrElse(k, 0L))).toMap
    def finish(map: Counts): String = if (!map.isEmpty) map.maxBy(_.swap)._1 else null
    def bufferEncoder: sql.Encoder[Counts] = implicitly(sql.catalyst.encoders.ExpressionEncoder[Counts])
    def outputEncoder: sql.Encoder[String] = implicitly(sql.catalyst.encoders.ExpressionEncoder[String])
  }

  def java_method(): String = {
    throw new AssertionError("java_method is not supported")
    "Never returned"
  }

  def register(reg: sql.UDFRegistration): Unit = {
    reg.register("geodistance", geodistance _)
    reg.register("hash", hash _)
    reg.register("most_common", org.apache.spark.sql.functions.udaf(mostCommon))
    reg.register("string_intersect", string_intersect _)
    reg.register("java_method", java_method _)
  }
}
