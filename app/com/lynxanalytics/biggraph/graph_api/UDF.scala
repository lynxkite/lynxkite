package com.lynxanalytics.biggraph.graph_api

import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.SQLContext
import com.lynxanalytics.biggraph.graph_operations

import com.vividsolutions.jts.geom

object UDF {
  def register(reg: UDFRegistration): Unit = {
    reg.register(
      "hash",
      (string: String, salt: String) => graph_operations.HashVertexAttribute.hash(string, salt))
    reg.register(
      "dayofweek",
      (date: String) => {
        import java.time.LocalDate
        import java.time.format.DateTimeFormatter
        val df = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        LocalDate.parse(date, df).getDayOfWeek.getValue()
      })
    reg.register(
      "geodistance",
      (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
        val factory = new geom.GeometryFactory()
        val p1 = factory.createPoint(new geom.Coordinate(lon1, lat1))
        val p2 = factory.createPoint(new geom.Coordinate(lon2, lat2))
        p1.distance(p2)
      })
  }
}
