package com.lynxanalytics.biggraph.graph_api.attributes

/* A trait that can read some value of type T from a DenseAttributes object.
 *
 * Similar to a AttributeReadIndex, but it is able to do some conversion/transformation
 * on the raw value and/or combine multiple attributes to a single value.
 */
trait AttributeReader[T] extends Serializable {
  def readFrom(attr: DenseAttributes): T
}
object AttributeReader {
  def getDoubleReader(sig: AttributeSignature, name: String)
      : Option[AttributeReader[Double]] = {
    if (sig.isReadableAs[Double](name)) {
      Some(new SimpleReader[Double](sig.getReadIndexFor[Double](name)))
    } else if (sig.isReadableAs[Long](name)) {
      Some(new ConvertedAttributeReader[Long, Double](
        sig.getReadIndexFor[Long](name), x => x.toDouble))
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
