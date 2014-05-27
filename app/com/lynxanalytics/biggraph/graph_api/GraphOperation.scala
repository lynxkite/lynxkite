package com.lynxanalytics.biggraph.graph_api

import attributes.AttributeSignature
import java.util.UUID
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

abstract class GraphOperation extends Serializable {
  def isSourceListValid(sources: Seq[BigGraph]): Boolean

  def execute(target: BigGraph, manager: GraphDataManager): GraphData

  // The vertex attribute signature of the graph resulting from this operation.
  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature

  // The edge attribute signature of the graph resulting from this operation.
  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature

  def targetProperties(sources: Seq[BigGraph]): BigGraphProperties = BigGraphProperties()

  def gUID(): UUID = {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(this)
    objectStream.close()
    return UUID.nameUUIDFromBytes(buffer.toByteArray)
  }
}
