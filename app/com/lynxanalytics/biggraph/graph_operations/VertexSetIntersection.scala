package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

object VertexSetIntersection {
  class Input(numVertexSets: Int) extends MagicInputSignature {
	val vss: List[VertexSetTemplate] = List.fill(numVertexSets)(vertexSet)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val intersection = vertexSet
  }
  
  def intersect(manager: MetaGraphManager, vss: VertexSet*): VertexSet = ???
    
//  def intersect(manager: MetaGraphManager, vss: VertexSet*): VertexSet = {
//    val inst = manager.apply(
//      VertexSetIntersection(vss.size),
//      vss.zipWithIndex.map { case (vs, idx) => (idx, vs) }: _*)
//    return inst.outputs.vertexSets('intersection)
//  }
}
import VertexSetIntersection._
case class VertexSetIntersection(numVertexSets: Int) extends TypedMetaGraphOp[Input, Output] {
  assert(numVertexSets >= 1)
  @transient override lazy val inputs = new Input(numVertexSets)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)  

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val intersection = inputs.vss.map(_.rdd)
      .reduce((rdd1, rdd2) => rdd1.join(rdd2).mapValues(_ => ()))
    output(o.intersection, intersection)
  }
}
