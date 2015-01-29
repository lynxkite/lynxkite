package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object Modularity extends OpFromJson {
  class Input() extends MagicInputSignature {
    val vs = vertexSet
    val segments = vertexSet
    val edgeIds = vertexSet
    val edges = edgeBundle(vs, vs, idSet = edgeIds)
    val weights = vertexAttribute[Double](edgeIds)
    val containedIn = edgeBundle(
      vs, segments, requiredProperties = EdgeBundleProperties.partialFunction)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input)
      extends MagicOutput(instance) {
    val modularity = scalar[Double]
  }
  def fromJson(j: JsValue) = Modularity()
}
import Modularity._
case class Modularity() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val vPart = inputs.vs.rdd.partitioner.get
    val vToS = inputs.containedIn.rdd.map { case (eid, e) => (e.src, e.dst) }.toSortedRDD(vPart)
    val bySrc = inputs.edges.rdd.sortedJoin(inputs.weights.rdd)
      .map { case (eid, (e, w)) => (e.src, (e.dst, w)) }
      .toSortedRDD(vPart)
    val byDst = bySrc.sortedJoin(vToS)
      .map { case (src, ((dst, w), srcS)) => (dst, (srcS, w)) }
      .toSortedRDD(vPart)
    val segmentEdges = byDst.sortedJoin(vToS)
      .map { case (dst, ((srcS, w), dstS)) => ((srcS, dstS), w) }
      .reduceBySortedKey(vPart, _ + _)
    val outDegSum = segmentEdges
      .map { case ((srcS, dstS), w) => (srcS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val inDegSum = segmentEdges
      .map { case ((srcS, dstS), w) => (dstS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val fullEdges = segmentEdges
      .filter { case ((srcS, dstS), w) => (srcS == dstS) }
      .map { case ((srcS, dstS), w) => (srcS, w) }
      .reduceBySortedKey(vPart, _ + _)
    val numEdges = outDegSum.values.reduce(_ + _)

    output(
      o.modularity,
      outDegSum.sortedJoin(inDegSum).fullOuterJoin(fullEdges)
        .map {
          case (s, (outsinsopt, fullsopt)) =>
            val (outs, ins) = outsinsopt.getOrElse((0.0, 0.0))
            val fulls = fullsopt.getOrElse(0.0)
            println(s"Processing $s. Ins: $ins outs: $outs fulls: $fulls")
            fulls / numEdges - ins / numEdges * outs / numEdges
        }
        .reduce(_ + _))
  }
}
