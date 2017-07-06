// Creates a new attribute by evaluating a Scala expression over other attributes.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect._
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.JavaScriptEvaluator
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.lynxanalytics.sandbox.ScalaScript

import org.apache.spark

object DeriveScala extends OpFromJson {
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
    val op = DeriveScala[Double]("-x", Seq("x"))
    op(op.attrs, Seq(x)).result.attr
  }

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
    val t = ScalaScript.compileAndGetType(
      exprString, paramTypes, paramsToOption = !onlyOnDefinedAttrs).payLoadType

    // The MetaGraph API expects to know the types of the outputs in compilation time,
    // so we cannot be more dynamic here.
    val a = namedAttributes.map(_._1)
    val s = namedScalars.map(_._1)
    val e = exprString
    val o = onlyOnDefinedAttrs
    val op = t match {
      case _ if t =:= typeOf[String] => DeriveScala[String](e, a, s, o)
      case _ if t =:= typeOf[Double] => DeriveScala[Double](e, a, s, o)
      case _ if t =:= typeOf[Vector[String]] => DeriveScala[Vector[String]](e, a, s, o)
      case _ if t =:= typeOf[Vector[Double]] => DeriveScala[Vector[Double]](e, a, s, o)
      case _ => throw new AssertionError(s"Unsupported result type of expression $exprString: $t")
    }

    import Scripting._
    op(op.vs, vertexSet)(
      op.attrs, namedAttributes.map(_._2))(
        op.scalars, namedScalars.map(_._2)).result.attr
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    implicit val tt = SerializableType.fromJson(j \ "type").typeTag
    DeriveScala(
      (j \ "expr").as[String],
      (j \ "attrNames").as[List[String]].toSeq,
      (j \ "scalarNames").as[List[String]].toSeq,
      (j \ "onlyOnDefinedAttrs").as[Boolean])
  }
}

// Derives a new AttributeRDD using the Scala expression (expr) and the input attribute and scalar
// values. The expression may return T or Option[T]. In the latter case the result RDD will be
// partially defined exactly where the expression returned Some[T]. The expression should never
// return nulls.
// If onlyOnDefinedAttrs is true then the expression is only evaluated for those records where all
// input attributes are present. If false, it's evaluated for every record, and the parameters will
// be substituted wrapped in Options.
import DeriveScala._
case class DeriveScala[T: TypeTag](
  expr: String, // The Scala expression to evaluate.
  attrNames: Seq[String], // Input attributes to substitute.
  scalarNames: Seq[String] = Seq(), // Input scalars to substitute.
  onlyOnDefinedAttrs: Boolean = true)
    extends TypedMetaGraphOp[Input, Output[T]] {

  def tt = typeTag[T]
  def st = SerializableType(tt)
  implicit def ct = RuntimeSafeCastable.classTagFromTypeTag(tt)
  override def toJson = Json.obj(
    "type" -> st.toJson,
    "expr" -> expr,
    "attrNames" -> attrNames,
    "scalarNames" -> scalarNames,
    "onlyOnDefinedAttrs" -> onlyOnDefinedAttrs)

  override val isHeavy = true
  @transient override lazy val inputs = new Input(attrNames.size, scalarNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output[T]()(instance, inputs, tt)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val joined: spark.rdd.RDD[(ID, Array[Any])] = {
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
                attrs(idx) = attr
                attrs
            }
        }
      }
    }

    val scalars = inputs.scalars.map { _.value }.toArray
    val allNames = attrNames ++ scalarNames
    val paramTypes = (
      attrNames.zip(inputs.attrs).map { case (k, v) => k -> v.data.typeTag } ++
      scalarNames.zip(inputs.scalars).map { case (k, v) => k -> v.data.typeTag })
      .toMap[String, TypeTag[_]]

    val t = ScalaScript.compileAndGetType(expr, paramTypes, paramsToOption = !onlyOnDefinedAttrs)
    assert(t.payLoadType =:= tt.tpe, // PayLoadType should always match T.
      s"Scala script returns wrong type: expected ${tt.tpe} but got ${t.payLoadType} instead.")

    val returnsOptionType = t.isOptionType
    val derived = joined.mapPartitions({ it =>
      val evaluator = ScalaScript.compileAndGetEvaluator(
        expr, paramTypes, paramsToOption = !onlyOnDefinedAttrs)
      it.flatMap {
        case (key, values) =>
          val namedValues = allNames.zip(values ++ scalars).toMap
          val result = evaluator.evaluate(namedValues)
          assert(Option(result).nonEmpty, s"Scala script $expr returned null.")
          // The compiler in ScalaScript should guarantee that this is always correct. We filter
          // out None-s, so the result is always a fully defined RDD[(ID, T)].
          if (returnsOptionType) {
            // The script returns Option[T] so we wrap it to Option[ID -> T].
            result.asInstanceOf[Option[T]].map { r => key -> r }
          } else {
            // The script returns T so we wrap it to Some[ID -> T] for consistency.
            Some(key -> result.asInstanceOf[T])
          }
      }
    }, preservesPartitioning = true).asUniqueSortedRDD
    output(o.attr, derived)
  }
}

