package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object DeriveJS {
  class Input(numAttrCount: Int, strAttrCount: Int, vecAttrCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val numAttrs = (0 until numAttrCount).map(i => vertexAttribute[Double](vs, Symbol("numAttr-" + i)))
    val strAttrs = (0 until strAttrCount).map(i => vertexAttribute[String](vs, Symbol("strAttr-" + i)))
    val vecAttrs = (0 until vecAttrCount).map(i => vertexAttribute[Vector[Any]](vs, Symbol("vecAttr-" + i)))
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance,
                           inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }
  def add(a: VertexAttribute[Double],
          b: VertexAttribute[Double])(implicit manager: MetaGraphManager): VertexAttribute[Double] = {
    import Scripting._
    val op = DeriveJSDouble(JavaScript("a + b"), Seq("a", "b"), Seq(), Seq())
    op(op.numAttrs, Seq(a, b)).result.attr
  }
}
import DeriveJS._
abstract class DeriveJS[T](
  expr: JavaScript,
  numAttrNames: Seq[String],
  strAttrNames: Seq[String],
  vecAttrNames: Seq[String])
    extends TypedMetaGraphOp[Input, Output[T]] {
  implicit def tt: TypeTag[T]
  override val isHeavy = true
  @transient override lazy val inputs = new Input(numAttrNames.size, strAttrNames.size, vecAttrNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(tt, instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
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
    val vecJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[Vector[Any]]())
      inputs.vecAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val derived = numJoined.sortedJoin(strJoined).sortedJoin(vecJoined).mapValues {
      case ((nums, strs), vecs) =>
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
