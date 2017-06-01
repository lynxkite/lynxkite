package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.sql.jdbc.JdbcDialect

import java.sql.Types
import org.apache.spark.sql.types._

class OracleJdbcDialect extends JdbcDialect {
  def canHandle(url: String) = {
    url.startsWith("jdbc:oracle:")
  }

  override def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.NUMERIC) {
      val scale = if (null != md) md.build().getLong("scale") else 0L
      if (scale >= 0 && scale <= size && size > 0) {
        // Spark's OracleDialect has some fancy logic for handling "abnormal" Oracle numbers,
        // like the ones with a negative scale. Unfortunately they also break "normal" numbers.
        // Here we fix the type of "normal" numbers but let Spark handle the "abnormal" ones.
        // See: https://github.com/biggraph/biggraph/issues/5982
        Some(DecimalType(size, scale.toInt))
      } else {
        None
      }
    } else {
      None
    }
  }
}
