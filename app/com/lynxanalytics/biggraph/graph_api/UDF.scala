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

  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
  class MostCommon extends UserDefinedAggregateFunction {
    import org.apache.spark.sql.expressions.MutableAggregationBuffer
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    override def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", StringType) :: Nil)

    override def bufferSchema: StructType = StructType(
      StructField("map", MapType(StringType, LongType, valueContainsNull = false)) :: Nil)

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Map()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val map = buffer.getAs[Map[String, Long]](0)
      val key = input.getString(0)
      if (map.contains(key)) {
        buffer(0) = map + (key -> (map(key) + 1L))
      } else {
        buffer(0) = map + (key -> 1L)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Map[String, Long]](0) ++ buffer2.getAs[Map[String, Long]](0)
    }

    override def evaluate(buffer: Row): Any = {
      // Select the max key by value first then key. This still yields deterministic results
      // in case we have multiple elements with the same frequency.
      buffer.getAs[Map[String, Long]](0).maxBy(_.swap)._1
    }
  }
  def register(reg: UDFRegistration): Unit = {
    reg.register("hash", hash _)
    reg.register("dayofweek", dayofweek _)
    reg.register("geodistance", geodistance _)
    reg.register("most_common", new MostCommon)
  }
}
