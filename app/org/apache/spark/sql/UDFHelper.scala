package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

object UDFHelper {
  def udfRegistration(fr: FunctionRegistry) = new UDFRegistration(fr)
}
