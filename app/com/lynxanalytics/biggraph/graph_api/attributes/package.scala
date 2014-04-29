package com.lynxanalytics.biggraph.graph_api

package object attributes {
  /**
   * An AttributeIndex that's usable to get out a type T
   * from a DenseAttributes object.
   */
  type AttributeReadIndex[T] = AttributeIndex[_ <: T]

  /**
   * An AttributeIndex that's usable to put in a type T
   * into a DenseAttributes object.
   */
  type AttributeWriteIndex[T] = AttributeIndex[_ >: T]
}
