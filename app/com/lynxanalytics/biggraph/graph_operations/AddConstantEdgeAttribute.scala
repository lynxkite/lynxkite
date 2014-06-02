package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.graphx
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api

import graph_api._
import graph_api.attributes._

abstract class AddConstantEdgeAttribute[T] extends GraphOperation {
  implicit def tt: TypeTag[T]

  val attributeName: String
  val value: T

  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 1)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val source = target.sources.head
    val sourceData = manager.obtainData(source)

    val SignatureExtension(sig, cloner) = edgeExtension(source)
    val idx = sig.writeIndex[T](attributeName)
    val edges = sourceData.edges.map(e =>
      new graphx.Edge(e.srcId, e.dstId, cloner.clone(e.attr).set(idx, value)))
    return new SimpleGraphData(target, sc.union(sourceData.vertices), edges)
  }

  private def edgeExtension(input: BigGraph) = input.edgeAttributes.addAttribute[T](attributeName)

  def vertexAttributes(sources: Seq[BigGraph]) = sources.head.vertexAttributes

  def edgeAttributes(sources: Seq[BigGraph]) = edgeExtension(sources.head).signature

  override def targetProperties(sources: Seq[BigGraph]) = sources.head.properties
}

case class ConstantDoubleEdgeAttribute(
  attributeName: String,
  value: Double)
    extends AddConstantEdgeAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}
