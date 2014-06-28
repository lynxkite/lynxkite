package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class EdgeAttributeToString() extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('es, 'src -> 'dst, create = true)
    .inputEdgeAttribute[Any]('attr, 'es)
    .outputEdgeAttribute[String]('string, 'es)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val attr: AttributeRDD[_] = inputs.edgeAttributes('attr).rdd
    val string = attr.mapValues(_.toString)
    outputs.putEdgeAttribute[String]('string, string)
  }
}

case class EdgeAttributeToDouble() extends MetaGraphOperation {
  def signature = newSignature
    .inputEdgeBundle('es, 'src -> 'dst, create = true)
    .inputEdgeAttribute[String]('attr, 'es)
    .outputEdgeAttribute[Double]('double, 'es)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val attr = inputs.edgeAttributes('attr).runtimeSafeCast[String].rdd
    val double = attr.flatMapValues(stringValue =>
      if (stringValue != "") Some(stringValue.toDouble) else None)
    outputs.putEdgeAttribute[Double]('double, double)
  }
}

case class VertexAttributeToString() extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexAttribute[Any]('attr, 'vs, create = true)
    .outputVertexAttribute[String]('string, 'vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val attr: AttributeRDD[_] = inputs.vertexAttributes('attr).rdd
    val string = attr.mapValues(_.toString)
    outputs.putVertexAttribute[String]('string, string)
  }
}

case class VertexAttributeToDouble() extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexAttribute[String]('attr, 'vs, create = true)
    .outputVertexAttribute[Double]('double, 'vs)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val attr = inputs.vertexAttributes('attr).runtimeSafeCast[String].rdd
    val double = attr.flatMapValues(stringValue =>
      if (stringValue != "") Some(stringValue.toDouble) else None)
    outputs.putVertexAttribute[Double]('double, double)
  }
}
