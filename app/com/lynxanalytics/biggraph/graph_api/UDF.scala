package com.lynxanalytics.biggraph.graph_api

import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.SQLContext
import com.lynxanalytics.biggraph.graph_operations

object UDF {

  def hash(string: String, salt: String): String = graph_operations.HashVertexAttribute.hash(string, salt)

  // Returns the day of the week of the given argument as an integer value in the range 1-7,
  // where 1 represents Sunday.
  // TODO: remove this once we migrate to Spark 2.3.
  def dayofweek(date: String): Int = {
    import java.time.LocalDate
    import java.time.format.DateTimeFormatter
    val df = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    // Java's LocalDate returns the ISO standard (Monday: 1, Sunday: 7) but we need to convert to
    // Sunday: 1, Saturday: 7 to match Spark 2.3's dayofweek.
    LocalDate.parse(date, df).getDayOfWeek.getValue() % 7 + 1
  }

  val crs = org.geotools.referencing.CRS.decode("EPSG:4326")

  // Returns the geodetic distance between two lat/long coordinates in meters.
  def geodistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    import com.vividsolutions.jts.geom._
    import org.geotools.geometry.jts._
    import org.geotools.referencing._
    val gc = new GeodeticCalculator(UDF.crs)
    gc.setStartingPosition(JTS.toDirectPosition(new Coordinate(lat1, lon1), UDF.crs))
    gc.setDestinationPosition(JTS.toDirectPosition(new Coordinate(lat2, lon2), UDF.crs))
    gc.getOrthodromicDistance()
  }

  def register(reg: UDFRegistration): Unit = {
    reg.register("hash", hash _)
    reg.register("dayofweek", dayofweek _)
    reg.register("geodistance", geodistance _)
  }
}
