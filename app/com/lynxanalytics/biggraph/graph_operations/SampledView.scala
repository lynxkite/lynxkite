package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.controllers.FEVertex
import com.lynxanalytics.biggraph.graph_api._

case class SampledView(
    center: String,
    radius: Int,
    hasEdges: Boolean,
    hasSizes: Boolean,
    hasLabels: Boolean) extends MetaGraphOperation {

  val hasCenter = center.nonEmpty

  def signature = {
    var s = newSignature.inputVertexSet('vertices)
    if (hasEdges) s = s.inputEdgeBundle('edges, 'vertices -> 'vertices)
    if (hasSizes) s = s.inputVertexAttribute[Double]('sizeAttr, 'vertices)
    if (hasLabels) s = s.inputVertexAttribute[Any]('labelAttr, 'vertices)
    s = s.outputVertexSet('sample)
    s = s.outputVertexAttribute[Int]('feIdxs, 'sample)
    s = s.outputScalar[Seq[FEVertex]]('feVertices)
    s
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext) = {
    val vs = inputs.vertexSets('vertices).rdd
    val vsPart = vs.partitioner.get
    val c = if (hasCenter) {
      center.toLong
    } else {
      vs.keys.first
    }
    val itself = rc.sparkContext.parallelize(Seq(c -> ())).partitionBy(vsPart)
    val neighborhood = if (hasEdges) {
      val edges = inputs.edgeBundles('edges).rdd
      val neighbors = edges.values.flatMap {
        e => Iterator(e.src -> e.dst, e.dst -> e.src)
      }.partitionBy(vsPart)
      var collection = itself
      for (i <- 0 until radius) {
        collection = collection.join(neighbors).flatMap {
          case (v, ((), neighbor)) => Iterator(v -> (), neighbor -> ())
        }.distinct.partitionBy(vsPart)
      }
      collection
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
