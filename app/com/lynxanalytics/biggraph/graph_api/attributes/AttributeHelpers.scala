package com.lynxanalytics.biggraph.graph_api.attributes

import scala.reflect.runtime.universe._

object DoubleAttributeReader {
  def getDoubleReader(sig: AttributeSignature, name: String)
      : Option[AttributeReader[Double]] = {
    if (sig.canRead[Double](name)) {
      Some(new SimpleReader[Double](sig.readIndex[Double](name)))
    } else if (sig.canRead[Long](name)) {
      Some(new ConvertedAttributeReader[Long, Double](
        sig.readIndex[Long](name), x => x.toDouble))
    } else {
      None
    }
  }

  def getDoubleReaders(sig: AttributeSignature)
      : (Seq[String], Seq[AttributeReader[Double]]) = {
    sig.attributeSeq
      .flatMap({
        name => getDoubleReader(sig, name).map(reader => (name, reader))
      })
      .unzip
  }
}

class SimpleReader[T](idx: AttributeReadIndex[T]) extends AttributeReader[T] {
  def readFrom(attr: DenseAttributes): T = {
    attr(idx)
  }
}

class ConvertedAttributeReader[S,T](
    idx: AttributeReadIndex[S], conversion: S => T) extends AttributeReader[T] {
  def readFrom(attr: DenseAttributes): T = {
    return conversion(attr(idx))
  }
}

object AttributeUtil {
  /**
   * Returns all attributes of a given type with their values.
   * Convenience method, this cannot be used on workers as signatures are not
   * serializable, so normally they are not available in closures.
   * Also first getting the indexes and then using those to get value from the
   * individual DenseAttributes objects is faster.
   */
  def getAttributeValues[T](sig: AttributeSignature, attr: DenseAttributes)(implicit c: TypeTag[T])
      : Map[String, T] = {
    sig.getAttributesReadableAs[T]
      .map(name => (name, attr(sig.readIndex[T](name)))).toMap
  }
}
