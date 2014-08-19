package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Merges vertices that match on an attribute.
case class MergeVertices[T]() extends TypedMetaGraphOp[VertexAttributeInput[T], Segmentation] {
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new Segmentation(inputs.vs.entity)
  }

  def execute(inputDatas: DataSet,
              o: Segmentation,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    val partitioner = rc.defaultPartitioner
    val byAttr = inputs.attr.rdd.map { case (id, attr) => (attr, id) }
    val matching = byAttr.groupByKey(partitioner)
    output(o.segments, matching.map {
      case (attr, vertices) => vertices.min -> ()
    }.toSortedRDD(partitioner))
    output(o.belongsTo, matching.flatMap {
      case (attr, vertices) =>
        val newID = vertices.min
        vertices.map(v => v -> Edge(v, newID))
    }.toSortedRDD(partitioner))
  }
}
