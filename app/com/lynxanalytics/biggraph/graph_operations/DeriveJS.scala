package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object DeriveJS {
  class Input(numAttrCount: Int, strAttrCount: Int) extends MagicInputSignature {
    val vs = vertexSet
    val numAttrs = (0 until numAttrCount).map(i => vertexAttribute[Double](vs, Symbol("numAttr-" + i)))
    val strAttrs = (0 until strAttrCount).map(i => vertexAttribute[String](vs, Symbol("strAttr-" + i)))
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[String](inputs.vs.entity)
  }
}
import DeriveJS._
case class DeriveJS(expr: JavaScript, numAttrNames: Seq[String], strAttrNames: Seq[String])
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input(numAttrNames.size, strAttrNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val numJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[Double]())
      inputs.numAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val strJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[String]())
      inputs.strAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val derived = numJoined.sortedJoin(strJoined).mapValues {
      case (nums, strs) =>
        val numValues = numAttrNames.zip(nums).toMap
        val strValues = strAttrNames.zip(strs).toMap
        expr.evaluate(numValues ++ strValues).toString
    }
    output(o.attr, derived)
  }
}
