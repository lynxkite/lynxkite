package com.lynxanalytics.biggraph.graph_api

import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.SQLContext
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

  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
  /**
   * Custom aggregator for SQL queries.
   *
   * Computes the most frequent value in a String column.
   * Null values are not counted.
   */
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
      buffer(0) = Map[String, Long]()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val map = buffer.getAs[Map[String, Long]](0)
      Option(input.getString(0)) match {
        case Some(key) =>
          buffer(0) = map + (key -> (map.getOrElse(key, 0L) + 1L))
        case None =>
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Map[String, Long]](0) ++ buffer2.getAs[Map[String, Long]](0)
    }

    override def evaluate(buffer: Row): Any = {
      // Select the max key by value first then key. This still yields deterministic results
      // in case we have multiple elements with the same frequency.
      val map = buffer.getAs[Map[String, Long]](0)
      if (!map.isEmpty) {
        map.maxBy(_.swap)._1
      } else {
        null
      }
    }
  }

  def register(reg: UDFRegistration): Unit = {
    reg.register("geodistance", geodistance _)
    reg.register("hash", hash _)
    reg.register("most_common", new MostCommon)
    reg.register("string_intersect", string_intersect _)
  }
}
