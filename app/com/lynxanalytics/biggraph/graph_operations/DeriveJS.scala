package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object DeriveJS {
  type ByType = (Seq[Double], Seq[String], Seq[Vector[Any]])

  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance,
                           inputs: VertexAttributeInput[ByType])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }

  def add(a: VertexAttribute[Double],
          b: VertexAttribute[Double])(implicit manager: MetaGraphManager): VertexAttribute[Double] = {
    import Scripting._
    val joined = {
      val op = JoinMoreAttributes(2, 0, 0)
      op(op.numAttrs, Seq(a, b)).result.attr
    }
    val op = DeriveJSDouble(JavaScript("a + b"), Seq("a", "b"), Seq(), Seq())
    op(op.attr, joined).result.attr
  }

  def negative(x: VertexAttribute[Double])(implicit manager: MetaGraphManager): VertexAttribute[Double] = {
    import Scripting._
    val joined = {
      val op = JoinMoreAttributes(1, 0, 0)
      op(op.numAttrs, Seq(x)).result.attr
    }
    val op = DeriveJSDouble(JavaScript("-x"), Seq("x"), Seq(), Seq())
    op(op.attr, joined).result.attr
  }
}
import DeriveJS._
abstract class DeriveJS[T](
  expr: JavaScript,
  numAttrNames: Seq[String],
  strAttrNames: Seq[String],
  vecAttrNames: Seq[String])
    extends TypedMetaGraphOp[VertexAttributeInput[ByType], Output[T]] {
  implicit def tt: TypeTag[T]
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[ByType]()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(tt, instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val derived = inputs.attr.rdd.mapValues {
      case (nums, strs, vecs) =>
        val numValues = numAttrNames.zip(nums).toMap
        val strValues = strAttrNames.zip(strs).toMap
        // There is no autounboxing in Javascript. So we unbox primitive arrays.
        val arrays = vecs.map { v =>
          if (v.forall(_.isInstanceOf[Double])) v.map(_.asInstanceOf[Double]).toArray
          else v.toArray
        }
        val vecValues: Map[String, Array[_]] = vecAttrNames.zip(arrays).toMap
        expr.evaluate(numValues ++ strValues ++ vecValues).asInstanceOf[T]
    }
    output(o.attr, derived)
  }
}

case class DeriveJSString(
  expr: JavaScript,
  numAttrNames: Seq[String],
  strAttrNames: Seq[String],
  vecAttrNames: Seq[String])
    extends DeriveJS[String](expr, numAttrNames, strAttrNames, vecAttrNames) {
  @transient lazy val tt = typeTag[String]
}

case class DeriveJSDouble(
  expr: JavaScript,
  numAttrNames: Seq[String],
  strAttrNames: Seq[String],
  vecAttrNames: Seq[String])
    extends DeriveJS[Double](expr, numAttrNames, strAttrNames, vecAttrNames) {
  @transient lazy val tt = typeTag[Double]
}
