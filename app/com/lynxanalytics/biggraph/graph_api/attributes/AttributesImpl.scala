package com.lynxanalytics.biggraph.graph_api.attributes

import scala.reflect.runtime.universe._

private[attributes] case class TypedAttributeIndex[T](idx: Int, tt: TypeTag[T]) {
  def forClassRead[S](st: TypeTag[S]): AttributeReadIndex[S] = {
    if (tt.tpe <:< st.tpe)
      new AttributeIndex[T](idx).asInstanceOf[AttributeReadIndex[S]]
    else throw new ClassCastException(
      "Cannot cast from: %s to: %s".format(tt, st))
  }
  def forClassWrite[S](st: TypeTag[S]): AttributeWriteIndex[S] = {
    if (st.tpe <:< tt.tpe)
      new AttributeIndex[T](idx).asInstanceOf[AttributeWriteIndex[S]]
    else throw new ClassCastException(
      "Cannot cast from: %s to: %s".format(st, tt))
  }

  def readableAs(st: TypeTag[_]): Boolean = {
    return tt.tpe <:< st.tpe
  }

  def writableAs(st: TypeTag[_]): Boolean = {
    return st.tpe <:< tt.tpe
  }
}
