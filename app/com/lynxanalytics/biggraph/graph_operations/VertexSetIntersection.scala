package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

case class VertexSetIntersection(numVertexSets: Int) extends MetaGraphOperation {
  assert(numVertexSets >= 1)

  def signature = {
    var sig = newSignature
    (0 until numVertexSets).foreach(i => sig = sig.inputVertexSet(
      VertexSetIntersection.vsName(i)))
    sig.outputVertexSet('intersection)
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    var res = inputs.vertexSets('vs0).rdd
    (1 until numVertexSets).foreach { i =>
      res = res.join(inputs.vertexSets(VertexSetIntersection.vsName(i)).rdd).mapValues(_ => ())
    }
    outputs.putVertexSet('intersection, res)
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
