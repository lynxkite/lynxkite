package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

object UDFHelper {
  // This is needed because the constructor of UDFRegistration is package private.
  def udfRegistration(fr: FunctionRegistry) = new UDFRegistration(fr)
}
