// Hack to access some private ML methods.
package org.apache.spark.ml.attribute

object AttributeHelper {
  def nominalAttribute(index: Int, values: Array[String]) = {
    new NominalAttribute(
      index = Some(index),
      isOrdinal = Some(false),
      values = Some(values))
  }

  def numericAttribute(index: Int) = {
    new NumericAttribute(
      index = Some(index))
  }
}
