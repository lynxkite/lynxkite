package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark.SparkContext.rddToPairRDDFunctions

object CreateVertexSet extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
  }
  def fromJson(j: play.api.libs.json.JsValue) = CreateVertexSet((j \ "size").as[Long])
}
import CreateVertexSet._
case class CreateVertexSet(size: Long) extends TypedMetaGraphOp[NoInput, Output] {
  @transient override lazy val inputs = new NoInput

  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val vertices: Seq[(ID, Unit)] = Seq.range[ID](0, size).map(x => (x, ()))
    output(o.vs, rc.sparkContext.parallelize(vertices).toSortedRDD(rc.defaultPartitioner))
  }
}
