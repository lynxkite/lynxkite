package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

case class VertexSetIntersection(numVertexSets: Int) extends MetaGraphOperation {
  assert(numVertexSets >= 1)
  import VertexSetIntersection.vsName

  def signature = {
    var sig = newSignature
    for (i <- (0 until numVertexSets)) {
      sig = sig.inputVertexSet(vsName(i))
    }
    sig.outputVertexSet('intersection)
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    //var res = inputs.vertexSets('vs0).rdd
    //for (i <- (1 until numVertexSets)) {
    //  res = res.join(inputs.vertexSets(vsName(i)).rdd).mapValues(_ => ())
    //}
    //outputs.putVertexSet('intersection, res)
    outputs.putVertexSet(
      'intersection,
      (0 until numVertexSets)
        .map(i => inputs.vertexSets(vsName(i)).rdd)
        .reduce((rdd1, rdd2) => rdd1.join(rdd2).mapValues(_ => ())))
  }
}
object VertexSetIntersection {
  private def vsName(i: Int) = Symbol(s"vs$i")

  def intersect(manager: MetaGraphManager, vss: VertexSet*): VertexSet = {
    val inst = manager.apply(
      VertexSetIntersection(vss.size),
      vss.zipWithIndex.map { case (vs, idx) => (vsName(idx), vs) }: _*)
    return inst.outputs.vertexSets('intersection)
  }
}
