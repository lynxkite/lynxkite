package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.graphx
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.spark_util._

case class CreateVertexSet(size: Long) extends MetaGraphOperation {
  def signature = newSignature
    .outputVertexSet('vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vertices: Seq[(ID, Unit)] = Seq.range[ID](0, size).map(x => (x, ()))
    outputs.putVertexSet('vs, rc.sparkContext.parallelize(vertices))
  }
}

case class SimpleRandomEdgeBundle(seed: Int, density: Float) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vsSrc)
    .inputVertexSet('vsDst)
    .outputEdgeBundle('es, 'vsSrc -> 'vsDst)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val rand = new Random(seed)
    val edges = for {
      src <- inputs.vertexSets('vsSrc).rdd
      dst <- inputs.vertexSets('vsDst).rdd.collect
      if rand.nextFloat < density
    } yield Edge(src._1, dst._1)
    outputs.putEdgeBundle('es, RDDUtils.fastNumbered(edges))
  }
}

case class SimpleRandomGraph(size: Int, seed: Int, density: Float) extends GraphOperation {
  def isSourceListValid(sources: Seq[BigGraph]): Boolean = sources.isEmpty

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val rand = new Random(seed)
    val maker = AttributeSignature.empty.maker
    val vertexIds = (0 until size).map(_.toLong)
    val vertices = vertexIds.map(id => (id, maker.make))
    val edges = for {
      src <- vertexIds
      dst <- vertexIds
      if rand.nextFloat < density
    } yield new graphx.Edge(src, dst, maker.make)
    return new SimpleGraphData(target, sc.parallelize(vertices), sc.parallelize(edges))
  }

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature = AttributeSignature.empty
}
