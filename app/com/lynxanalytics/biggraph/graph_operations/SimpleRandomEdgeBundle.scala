package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._

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
