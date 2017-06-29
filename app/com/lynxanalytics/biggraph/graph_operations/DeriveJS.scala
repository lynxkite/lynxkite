// Creates a new attribute by evaluating a JavaScript expression over other attributes.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.JavaScriptEvaluator
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.lynxanalytics.sandbox.ScalaScript

object DeriveJS {
  class Input(attrCount: Int, scalarCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val attrs = (0 until attrCount).map(i => anyVertexAttribute(vs, Symbol("attr-" + i)))
    val scalars = (0 until scalarCount).map(i => anyScalar(Symbol("scalar-" + i)))
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input, tt: TypeTag[T]) extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }
  def negative(x: Attribute[Double])(implicit manager: MetaGraphManager): Attribute[Double] = {
    import Scripting._
    val op = DeriveJS[Double]("-x", Seq("x"))
    op(op.attrs, Seq(x).map(VertexAttributeToJSValue.run[Double])).result.attr
  }

  val resultTypeWhiteList = Seq(
    typeTag[String],
    typeTag[Double],
    typeTag[Vector[String]],
    typeTag[Vector[Double]])

  def deriveFromAttributes(
    exprString: String,
    namedAttributes: Seq[(String, Attribute[_])],
    vertexSet: VertexSet,
    namedScalars: Seq[(String, Scalar[_])] = Seq(),
    onlyOnDefinedAttrs: Boolean = true)(
      implicit manager: MetaGraphManager): Attribute[_] = {

    // Check name collision between scalars and attributes
    val common =
      namedAttributes.map(_._1).toSet & namedScalars.map(_._1).toSet
    assert(common.isEmpty, {
      val collisions = common.mkString(",")
      s"Identical scalar and attribute name: $collisions." +
        s" Please rename either the scalar or the attribute."
    })

    val paramTypes = (
      namedAttributes.map { case (k, v) => k -> v.typeTag } ++
      namedScalars.map { case (k, v) => k -> v.typeTag }).toMap[String, TypeTag[_]]
    val expressionTypeTag = ScalaScript.inferType(exprString, paramTypes)
    val resultTypeTag = resultTypeWhiteList.find(_.tpe =:= expressionTypeTag.tpe)
    assert(resultTypeTag.nonEmpty, s"Unsupported result type of expression $exprString: $expressionTypeTag")

    val a = namedAttributes.map(_._1)
    val s = namedScalars.map(_._1)
    val e = exprString
    val o = onlyOnDefinedAttrs
    val t = resultTypeTag.get.tpe
    val op = t match {
      case _ if t =:= typeOf[String] => DeriveJS[String](e, a, s, o)
      case _ if t =:= typeOf[Double] => DeriveJS[Double](e, a, s, o)
      case _ if t =:= typeOf[Vector[String]] => DeriveJS[Vector[String]](e, a, s, o)
      case _ if t =:= typeOf[Vector[Double]] => DeriveJS[Vector[Double]](e, a, s, o)
    }

    import Scripting._
    op(op.vs, vertexSet)(
      op.attrs, namedAttributes.map(_._2))(
        op.scalars, namedScalars.map(_._2)).result.attr
  }
}
import DeriveJS._
case class DeriveJS[T: TypeTag](
  expr: String,
  attrNames: Seq[String],
  scalarNames: Seq[String] = Seq(),
  onlyOnDefinedAttrs: Boolean = true)
    extends TypedMetaGraphOp[Input, Output[T]] {
  val tt = typeTag[T]
  implicit def ct = RuntimeSafeCastable.classTagFromTypeTag(tt)
  override val isHeavy = true
  @transient override lazy val inputs = new Input(attrNames.size, scalarNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output[T]()(instance, inputs, tt)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val joined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => new Array[Any](attrNames.size))
      if (onlyOnDefinedAttrs) {
        inputs.attrs.zipWithIndex.foldLeft(noAttrs) {
          case (rdd, (attr, idx)) =>
            rdd.sortedJoin(attr.rdd).mapValues {
              case (attrs, attr) =>
                attrs(idx) = attr
                attrs
            }
        }
      } else {
        inputs.attrs.zipWithIndex.foldLeft(noAttrs) {
          case (rdd, (attr, idx)) =>
            rdd.sortedLeftOuterJoin(attr.rdd).mapValues {
              case (attrs, attr) =>
                attrs(idx) = attr.getOrElse(null)
                attrs
            }
        }
      }
    }

    val scalars = inputs.scalars.map { _.value }.toArray
    val allNames = attrNames ++ scalarNames
    val paramTypes = (
      inputs.attrs.map { attr => attr.name.toString -> attr.data.typeTag } ++
      inputs.scalars.map { scalar => scalar.name.toString -> scalar.data.typeTag })
      .toMap[String, TypeTag[_]]

    val derived = joined.mapPartitions({ it =>
      val evaluator = ScalaScript.getEvaluator(expr, paramTypes)
      it.map {
        case (key, values) =>
          val namedValues = allNames.zip(values ++ scalars).toMap
          // It would be nice to do an extra type check here but we can only get the
          // runtime types, after generic type erasure.
          key -> evaluator.evaluate(namedValues).asInstanceOf[T]
      }
    }, preservesPartitioning = true).asUniqueSortedRDD
    output(o.attr, derived)
  }
}

