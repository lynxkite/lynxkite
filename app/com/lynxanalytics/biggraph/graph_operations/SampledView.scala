package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.controllers.FEVertex
import com.lynxanalytics.biggraph.graph_api._

case class SampledView(
    centralVertexId: String,
    radius: Int,
    hasEdges: Boolean,
    hasSizes: Boolean,
    hasLabels: Boolean) extends MetaGraphOperation {

  val hasCenter = centralVertexId.nonEmpty

  def signature = {
    var s = newSignature.inputVertexSet('vertices)
    if (hasEdges) s = s.inputEdgeBundle('edges, 'vertices -> 'vertices)
    if (hasSizes) s = s.inputVertexAttribute[Double]('sizeAttr, 'vertices)
    if (hasLabels) s = s.inputVertexAttribute[Any]('labelAttr, 'vertices)
    s = s.outputVertexSet('sample)
    s = s.outputScalar[Seq[FEVertex]]('feVertices)
    s
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val vs = inputs.vertexSets('vertices).rdd
    val vsPart = vs.partitioner.get
    val c = if (hasCenter) {
      centralVertexId.toLong
    } else {
      vs.keys.first
    }
    val itself = rc.sparkContext.parallelize(Seq(c -> ())).partitionBy(vsPart)
    val neighborhood = if (hasEdges) {
      val edges = inputs.edgeBundles('edges).rdd
      val outgoing = edges.values.map(e => e.src -> e.dst).partitionBy(vsPart)
      val incoming = edges.values.map(e => e.dst -> e.src).partitionBy(vsPart)
      var collection = itself
      for (i <- 0 until radius) {
        val inNeighbors = collection.join(incoming).map {
          case (v, ((), src)) => src -> ()
        }
        val outNeighbors = collection.join(outgoing).map {
          case (v, ((), dst)) => dst -> ()
        }
        collection ++= inNeighbors
        collection ++= outNeighbors
      }
      collection.distinct
    } else {
      itself
    }

    val sizes = if (hasSizes) {
      inputs.vertexAttributes('sizeAttr).runtimeSafeCast[Double].rdd
    } else {
      neighborhood.mapValues(_ => 1.0)
    }
    val labels = if (hasLabels) {
      inputs.vertexAttributes('labelAttr).runtimeSafeCast[String].rdd
    } else {
      neighborhood.mapValues(_ => "")
    }
    val feVertices = neighborhood.join(sizes).join(labels).map {
      case (id, (((), size), label)) => FEVertex(id = id, size = size, label = label)
    }.collect.toSeq
    val idxs = feVertices.zipWithIndex.map {
      case (v, idx) => v.id -> idx
    }
    val idxRDD = rc.sparkContext.parallelize(idxs).partitionBy(vsPart)

    outputs.putVertexSet('sample, neighborhood)
    outputs.putVertexAttribute('feIdxs, idxRDD)
    outputs.putScalar('feVertices, feVertices)
  }
}
