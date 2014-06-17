package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._

case class CreateVertexSet(size: Long) extends MetaGraphOperation {
  def signature = newSignature
    .outputVertexSet('vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vertices: Seq[(ID, Unit)] = Seq.range[ID](0, size).map(x => (x, ()))
    outputs.putVertexSet('vs, rc.sparkContext.parallelize(vertices))
  }
}
