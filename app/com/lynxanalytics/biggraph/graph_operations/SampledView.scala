package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

case class SampledViewVertex(id: Long, size: Double, label: String)

object SampledView {
  class Input(hasEdges: Boolean, hasSizes: Boolean, hasLabels: Boolean) extends MagicInputSignature {
    val vertices = vertexSet
    val edges = if (hasEdges) edgeBundle(vertices, vertices) else null
    val sizeAttr = if (hasSizes) vertexAttribute[Double](vertices) else null
    val labelAttr = if (hasLabels) vertexAttribute[String](vertices) else null
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sample = vertexSet
    val feIdxs = vertexAttribute[Int](sample)
    val svVertices = scalar[Seq[SampledViewVertex]]
  }
}
import SampledView._

case class SampledView(
    center: String,
    radius: Int,
    hasEdges: Boolean,
    hasSizes: Boolean,
    hasLabels: Boolean) extends TypedMetaGraphOp[Input, Output] {

  val hasCenter = center.nonEmpty
  @transient override lazy val inputs = new Input(hasEdges, hasSizes, hasLabels)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val vs = inputs.vertices.rdd
    val vsPart = vs.partitioner.get
    val c = if (hasCenter) {
      center.toLong
    } else {
      vs.keys.first
    }
    val itself = rc.sparkContext.parallelize(Seq(c -> ())).partitionBy(vsPart)
    val neighborhood = if (hasEdges) {
      val neighbors = inputs.edges.rdd.values.flatMap {
        e => Iterator(e.src -> e.dst, e.dst -> e.src)
      }.partitionBy(vsPart)
      var collection = itself
      for (i <- 0 until radius) {
        collection = collection.leftOuterJoin(neighbors).flatMap {
          case (v, ((), None)) => Iterator(v -> ())
          case (v, ((), Some(neighbor))) => Iterator(v -> (), neighbor -> ())
        }.distinct.partitionBy(vsPart)
      }
      collection
    } else {
      itself
    }

    val sizes = if (hasSizes) {
      inputs.sizeAttr.rdd
    } else {
      neighborhood.mapValues(_ => 1.0)
    }
    val labels = if (hasLabels) {
      inputs.labelAttr.rdd
    } else {
      neighborhood.mapValues(_ => "")
    }
    val svVertices = neighborhood.join(sizes).join(labels).map {
      case (id, (((), size), label)) => SampledViewVertex(id, size, label)
    }.collect.toSeq
    val idxs = svVertices.zipWithIndex.map {
      case (v, idx) => v.id -> idx
    }
    val idxRDD = rc.sparkContext.parallelize(idxs).partitionBy(vsPart)

    output(o.sample, neighborhood)
    output(o.feIdxs, idxRDD)
    output(o.svVertices, svVertices)
  }
}
