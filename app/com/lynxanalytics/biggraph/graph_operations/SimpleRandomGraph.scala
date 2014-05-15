package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.graphx
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature

case class SimpleRandomGraph(size: Int, seed: Int, density: Float) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val rand = new Random(seed)
    val maker = AttributeSignature.empty.maker
    val vertexIds = (0 until size).map(_.toLong)
    val vertices = vertexIds.map(id => (id, maker.make))
    val edges = for (
        src <- vertexIds;
        dst <- vertexIds;
        if rand.nextFloat < density) yield new graphx.Edge(src, dst, maker.make)
    return new SimpleGraphData(target, sc.parallelize(vertices), sc.parallelize(edges))
  }

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature  = AttributeSignature.empty
}
