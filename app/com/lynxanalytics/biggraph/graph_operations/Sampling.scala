package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

case class VertexSample(fraction: Double) extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vertices)
    .outputVertexSet('sampled)
    .outputEdgeBundle('projection, 'vertices -> 'sampled)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val vertices = inputs.vertexSets('vertices).rdd
    val sampled = vertices.sample(withReplacement = false,
      fraction = fraction,
      seed = 0)
    outputs.putVertexSet('sampled, sampled)
    outputs.putEdgeBundle(
      'projection,
      RDDUtils.fastNumbered(sampled.map { case (id, _) => Edge(id, id) })
        .partitionBy(rc.defaultPartitioner))
  }
}

abstract class SampledVertexAttribute[T]() extends MetaGraphOperation {
  implicit def tt: TypeTag[T]

  def signature = newSignature
    .inputVertexAttribute[T]('attribute, 'vertices, create = true)
    // For now it's up to the user to guarantee that 'sampled is indeed a sample of 'vertices.
    .inputVertexSet('sampled)
    .outputVertexAttribute[T]('sampled_attribute, 'sampled)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val sampled = inputs.vertexSets('sampled).rdd
    val attribute = inputs.vertexAttributes('attribute).runtimeSafeCast[T].rdd
    outputs.putVertexAttribute(
      'sampled_attribute,
      sampled.join(attribute).mapValues { case (_, value) => value })
  }
}

case class SampledDoubleVertexAttribute() extends SampledVertexAttribute[Double] {
  @transient lazy val tt = typeTag[Double]
}
