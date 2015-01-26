package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EmptyEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val eb = edgeBundle(inputs.src.entity, inputs.dst.entity)
  }
  def fromJson(j: JsValue) = EmptyEdgeBundle()
}
import EmptyEdgeBundle._
case class EmptyEdgeBundle() extends TypedMetaGraphOp[Input, Output] {
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    output(o.eb, rc.sparkContext.emptyRDD[(ID, Edge)].toSortedRDD(rc.defaultPartitioner))
  }
}
