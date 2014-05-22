package com.lynxanalytics.biggraph.graph_api.attributes

import scala.reflect.runtime.universe._

private[attributes] case class TypedAttributeIndex[T: TypeTag](idx: Int) {
  def forClassRead[S: TypeTag]: AttributeReadIndex[S] = {
    if (typeOf[T] <:< typeOf[S])
      new AttributeIndex[T](idx).asInstanceOf[AttributeReadIndex[S]]
    else throw new ClassCastException(
      "Cannot cast from: %s to: %s".format(typeOf[T], typeOf[S]))
  }
  def forClassWrite[S: TypeTag]: AttributeWriteIndex[S] = {
    if (typeOf[S] <:< typeOf[T])
      new AttributeIndex[T](idx).asInstanceOf[AttributeWriteIndex[S]]
    else throw new ClassCastException(
      "Cannot cast from: %s to: %s".format(typeOf[S], typeOf[T]))
  }

  def readableAs[S: TypeTag]: Boolean = {
    return typeOf[T] <:< typeOf[S]
  }

  def writableAs[S: TypeTag]: Boolean = {
    return typeOf[S] <:< typeOf[T]
  }

  def getReaderForOperation[S](op: TypeDependentOperation[S]): AttributeReader[S] = {
    op.getReaderForIndex(new AttributeIndex[T](idx))
  }
}
