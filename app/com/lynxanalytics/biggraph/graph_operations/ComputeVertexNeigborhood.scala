package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.graph_api._

object ComputeVertexNeighborhood {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val edges = edgeBundle(vertices, vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val neighborsIdToIndex = scalar[Map[ID, Int]]
  }
}
import ComputeVertexNeighborhood._
case class ComputeVertexNeighborhood(
    center: ID,
    radius: Int) extends TypedMetaGraphOp[Input, Output] {

  @transient override lazy val inputs = new Input

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    implicit val id = inputDatas
    val vs = inputs.vertices.rdd
    val es = inputs.edges.rdd
    val vsPart = vs.partitioner.get
    var neigborhood = Set(center)
    for (i <- 0 until radius) {
      neigborhood ++= es
        .values
        .filter(e => (neigborhood.contains(e.src) != neigborhood.contains(e.dst)))
        .flatMap(e => Iterator(e.src, e.dst))
        .distinct
        .collect
        .toSet
    }
    output(o.neighborsIdToIndex, neigborhood.zipWithIndex.toMap)
  }
}
