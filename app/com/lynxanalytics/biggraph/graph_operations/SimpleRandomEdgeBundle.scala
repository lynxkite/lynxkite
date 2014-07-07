package com.lynxanalytics.biggraph.graph_operations

import scala.util.Random

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils.Implicit

case class SimpleRandomEdgeBundle(seed: Int, density: Float) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vsSrc)
    .inputVertexSet('vsDst)
    .outputEdgeBundle('es, 'vsSrc -> 'vsDst)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val allEdges = inputs.vertexSets('vsSrc).rdd.cartesian(inputs.vertexSets('vsDst).rdd)

    val randomEdges = allEdges.mapPartitionsWithIndex {
      case (pidx, it) =>
        val rand = new Random((pidx << 16) + seed)
        it.filter(_ => rand.nextFloat < density)
          .map { case ((srcId, _), (dstId, _)) => Edge(srcId, dstId) }
    }

    outputs.putEdgeBundle('es, randomEdges.fastNumbered.partitionBy(rc.defaultPartitioner))
  }
}
