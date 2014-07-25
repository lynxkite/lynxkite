package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._

object VertexSetIntersection {
  class Input(numVertexSets: Int) extends MagicInputSignature {
    val vss = Range(0, numVertexSets).map {
      i => vertexSet(Symbol("vs" + i))
    }.toList
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val intersection = vertexSet
  }
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
