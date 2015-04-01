package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

// Creates a new edge bundle that connects all vertices inside each segment.
// If two vertices co-occur multiple times, they will be connected multiple
// times. Loop edges are also generated.
object EdgesFromSegmentation extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val seg = vertexSet
    val belongsTo = edgeBundle(vs, seg)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               input: Input) extends MagicOutput(instance) {
    // Multiple co-occurrence is represented by parallel edges.
    val es = edgeBundle(input.vs.entity, input.vs.entity)
  }
  def fromJson(j: JsValue) = EdgesFromSegmentation()
}
import EdgesFromSegmentation._
case class EdgesFromSegmentation()
    extends TypedMetaGraphOp[Input, Output] {

  override val isHeavy = true
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val belongsTo = inputs.belongsTo.rdd
    val p = belongsTo.partitioner.get
    val segToVs = belongsTo.values.map(e => e.dst -> e.src).toSortedRDD(p)
    val edges = segToVs.groupByKey.values.flatMap { members =>
      for (v <- members; w <- members) yield Edge(v, w)
    }.randomNumbered(p)
    output(o.es, edges)
  }
}
