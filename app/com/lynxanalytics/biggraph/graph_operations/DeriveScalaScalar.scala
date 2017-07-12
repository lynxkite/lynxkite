// Creates a new scalar by evaluating a Scala expression over other scalars.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.sandbox.ScalaScript

object DeriveScalaScalar extends OpFromJson {
  class Input(s: Seq[(String, SerializableType[_])]) extends MagicInputSignature {
    val scalars = s.map(i => runtimeTypedScalar(Symbol(i._1), i._2.typeTag))
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val sc = scalar[T]
  }

  def deriveAndInferReturnType(
    exprString: String,
    namedScalars: Seq[(String, Scalar[_])])(implicit manager: MetaGraphManager): Scalar[_] = {

    val paramTypesMap = namedScalars.map { case (k, v) => k -> v.typeTag }.toMap[String, TypeTag[_]]
    val t = ScalaScript.compileAndGetType(
      exprString, paramTypesMap, paramsToOption = false).returnType

    val tt = SerializableType(t).typeTag
    derive(exprString, namedScalars)(tt, manager)
  }

  def derive[T: TypeTag](
    exprString: String,
    namedScalars: Seq[(String, Scalar[_])])(implicit manager: MetaGraphManager): Scalar[T] = {

    val paramTypes = namedScalars.map { case (k, v) => k -> v.typeTag }
    DeriveScala.checkInputTypes(paramTypes.toMap[String, TypeTag[_]], exprString)

    val tt = SerializableType(typeTag[T]).typeTag
    val op = DeriveScalaScalar(exprString, paramTypes)(tt)

    import Scripting._
    op(op.scalars, namedScalars.map(_._2)).result.sc.runtimeSafeCast[T]
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    implicit val tt = SerializableType.fromJson(j \ "type").typeTag
    DeriveScalaScalar(
      (j \ "expr").as[String],
      DeriveScala.jsonToParams(j \ "scalarNames"))
  }
}
import DeriveScalaScalar._
case class DeriveScalaScalar[T: TypeTag](
  expr: String,
  scalarParams: Seq[(String, TypeTag[_])])
    extends TypedMetaGraphOp[Input, Output[T]] {

  def tt = typeTag[T]
  def st = SerializableType(tt)
  implicit def ct = RuntimeSafeCastable.classTagFromTypeTag(tt)
  override def toJson = Json.obj(
    "type" -> st.toJson,
    "expr" -> expr,
    "scalarNames" -> DeriveScala.paramsToJson(scalarParams))

  override val isHeavy = true
  @transient override lazy val inputs = new Input(scalarParams.map { case (k, v) => k -> SerializableType(v) })
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(tt, instance)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val scalarValues = inputs.scalars.map(_.value)
    val paramTypes = scalarParams.toMap[String, TypeTag[_]]

    val t = ScalaScript.compileAndGetType(expr, paramTypes, paramsToOption = false)
    assert(t.returnType =:= tt.tpe,
      s"Scala script returns wrong type: expected ${tt.tpe} but got ${t.returnType} instead.")

    val evaluator = ScalaScript.compileAndGetEvaluator(expr, paramTypes, paramsToOption = false)
    val namedValues = scalarParams.map(_._1).zip(scalarValues).toMap
    val result = evaluator.evaluate(namedValues)
    assert(Option(result).nonEmpty, s"Scala script $expr returned null.")
    output(o.sc, result.asInstanceOf[T])
  }
}

