package com.lynxanalytics.biggraph.graph_api

import attributes.AttributeSignature
import java.util.UUID
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

abstract class GraphOperation extends Serializable {
  def areSourcesCompatible(sources: Seq[BigGraph]): Boolean

  def apply(sources: Seq[BigGraph], manager: GraphDataManager): GraphData

  // The vertex property signature of the graph resulting from
  // this operation.
  def vertexProperties(sources: Seq[BigGraph]): AttributeSignature

  // The edge property signature of the graph resulting from
  // this operation.
  def edgeProperties(sources: Seq[BigGraph]): AttributeSignature

  def gUID(): UUID = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(this)
    objectStream.close()
    return UUID.nameUUIDFromBytes(buffer.toByteArray)
  }
}
