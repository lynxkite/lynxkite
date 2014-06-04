package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.attributes.AttributeSignature
import com.lynxanalytics.biggraph.graph_api.attributes.DenseAttributes
import com.lynxanalytics.biggraph.graph_api.attributes.SignatureExtension

case class WeightedOutDegree(weightAttribute: String, outputAttribute: String)
    extends NewVertexAttributeOperation[Double] {
  @transient lazy val tt = typeTag[Double]

  override def isSourceListValid(sources: Seq[BigGraph]): Boolean =
    super.isSourceListValid(sources) && sources.head.edgeAttributes.canRead[Double](weightAttribute)

  override def computeHolistically(inputData: GraphData,
                                   runtimeContext: RuntimeContext,
                                   vertexPartitioner: spark.Partitioner): RDD[(VertexId, Double)] = {
    val readIdx = inputData.bigGraph.edgeAttributes.readIndex[Double](weightAttribute)
    inputData.edges
      .map(e => (e.srcId, e.attr(readIdx)))
      .reduceByKey(
        vertexPartitioner,
        _ + _)
  }

  override def computeLocally(vid: VertexId, da: DenseAttributes): Double = 0.0
}
