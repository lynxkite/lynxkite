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
  class Input(a: Seq[(String, SerializableType[_])], s: Seq[(String, SerializableType[_])])
      extends MagicInputSignature {
    val vs = vertexSet
    val attrs = a.map(i => runtimeTypedVertexAttribute(vs, Symbol(i._1), i._2.typeTag))
    val scalars = s.map(i => runtimeTypedScalar(Symbol(i._1), i._2.typeTag))
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input, tt: TypeTag[T]) extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }
  def negative(x: Attribute[Double])(implicit manager: MetaGraphManager): Attribute[Double] = {
    derive[Double]("-x", Seq("x" -> x))
  }

  // Derives a new AttributeRDD using the Scala expression (expr) and the input attribute and scalar
  // values. The expression may return T or Option[T]. In the latter case the result RDD will be
  // partially defined exactly where the expression returned Some[T]. The expression should never
  // return nulls.
  // If onlyOnDefinedAttrs is true then the expression is only evaluated for those records where all
  // input attributes are present. If false, it's evaluated for every record, and the parameters will
  // be substituted wrapped in Options.
  def derive[T: TypeTag](
    exprString: String,
    attributes: Seq[(String, Attribute[_])],
    scalars: Seq[(String, Scalar[_])] = Seq(),
    onlyOnDefinedAttrs: Boolean = true,
    vertexSet: Option[VertexSet] = None)(
      implicit manager: MetaGraphManager): Attribute[T] = {
    assert(attributes.nonEmpty || vertexSet.nonEmpty,
      "There should be either at least one attribute or vertexSet defined.")
    val attr = deriveFromAttributes(
      exprString,
      attributes,
      vertexSet.getOrElse(attributes.head._2.vertexSet),
      scalars,
      onlyOnDefinedAttrs)
    assert(attr.typeTag.tpe =:= typeOf[T])
    attr.runtimeSafeCast[T]
  }

  // Same as above but infers T from the input parameters and the script.
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

    val attrTypes = namedAttributes.map { case (k, v) => k -> v.typeTag }
    val scalarTypes = namedScalars.map { case (k, v) => k -> v.typeTag }
    val paramTypes = (attrTypes ++ scalarTypes).toMap[String, TypeTag[_]]
    checkInputTypes(paramTypes)

    val t = ScalaScript.compileAndGetType(
      exprString, paramTypes, paramsToOption = !onlyOnDefinedAttrs).payLoadType

    val tt = SerializableType(t).typeTag
    val op = DeriveScala(exprString, attrTypes, scalarTypes, onlyOnDefinedAttrs)(tt)

    import Scripting._
    op(op.vs, vertexSet)(
      op.attrs, namedAttributes.map(_._2))(
        op.scalars, namedScalars.map(_._2)).result.attr
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    implicit val tt = SerializableType.fromJson(j \ "type").typeTag
    DeriveScala(
      (j \ "expr").as[String],
      jsonToParams(j \ "attrNames"),
      jsonToParams(j \ "scalarNames"),
      (j \ "onlyOnDefinedAttrs").as[Boolean])
  }

  import play.api.libs.json.Json
  def paramsToJson(paramTypes: Seq[(String, TypeTag[_])]) = {
    paramTypes.map { case (n, t) => Json.obj("name" -> n, "type" -> SerializableType(t).toJson) }
  }

  def jsonToParams(j: JsValue) = {
    j.as[List[JsValue]].map { p => (p \ "name").as[String] -> SerializableType.fromJson(p \ "type").typeTag }
  }

  def checkInputTypes(paramTypes: Map[String, TypeTag[_]]): Unit = {
    paramTypes.foreach {
      case (k, t) =>
        try {
          SerializableType(t)
        } catch {
          case e: AssertionError => throw new AssertionError(
            s"Unsupported type $t for input parameter $k. Please don't use this parameter or change its type.")
        }
    }
  }
}

// Using this constructor directly is discouraged, please use the functions above.
import DeriveScala._
case class DeriveScala[T: TypeTag] private[graph_operations] (
  expr: String, // The Scala expression to evaluate.
  attrParams: Seq[(String, TypeTag[_])], // Input attributes to substitute.
  scalarParams: Seq[(String, TypeTag[_])] = Seq(), // Input scalars to substitute.
  onlyOnDefinedAttrs: Boolean = true)
    extends TypedMetaGraphOp[Input, Output[T]] {

  def tt = typeTag[T]
  def st = SerializableType(tt)
  implicit def ct = RuntimeSafeCastable.classTagFromTypeTag(tt)

  override def toJson = Json.obj(
    "type" -> st.toJson,
    "expr" -> expr,
    "attrNames" -> DeriveScala.paramsToJson(attrParams),
    "scalarNames" -> DeriveScala.paramsToJson(scalarParams),
    "onlyOnDefinedAttrs" -> onlyOnDefinedAttrs)

  override val isHeavy = true
  @transient override lazy val inputs = new Input(
    attrParams.map { case (k, v) => k -> SerializableType(v) },
    scalarParams.map { case (k, v) => k -> SerializableType(v) })
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output[T]()(instance, inputs, tt)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val joined: spark.rdd.RDD[(ID, Array[Any])] = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => new Array[Any](attrParams.size))
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

    val scalarValues = inputs.scalars.map { _.value }.toArray
    val allNames = (attrParams.map(_._1) ++ scalarParams.map(_._1)).toSeq
    val paramTypes = (attrParams ++ scalarParams).toMap[String, TypeTag[_]]

    val t = ScalaScript.compileAndGetType(expr, paramTypes, paramsToOption = !onlyOnDefinedAttrs)
    assert(t.payLoadType =:= tt.tpe, // PayLoadType should always match T.
      s"Scala script returns wrong type: expected ${tt.tpe} but got ${t.payLoadType} instead.")

    val returnsOptionType = t.isOptionType
    val derived = joined.mapPartitions({ it =>
      val evaluator = ScalaScript.compileAndGetEvaluator(
        expr, paramTypes, paramsToOption = !onlyOnDefinedAttrs)
      it.flatMap {
        case (key, values) =>
          val namedValues = allNames.zip(values ++ scalarValues).toMap
          val result = evaluator.evaluate(namedValues)
          assert(result != null, s"Scala script $expr returned null.")
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

