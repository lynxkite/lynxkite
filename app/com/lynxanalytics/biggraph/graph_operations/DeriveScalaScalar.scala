// Creates a new scalar by evaluating a Scala expression over other scalars.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.sandbox.ScalaScript

object DeriveScalaScalar extends OpFromJson {
  class Input(s: Seq[(String, SerializableType[_])]) extends MagicInputSignature {
    val scalars = s.map(i => scalarT(Symbol(i._1))(i._2.typeTag).asInstanceOf[ScalarTemplate[Any]])
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val sc = scalar[T]
  }

  def deriveFromScalars(
    exprString: String,
    namedScalars: Seq[(String, Scalar[_])])(implicit manager: MetaGraphManager): Output[_] = {
    val paramTypes =
      namedScalars.map { case (k, v) => k -> v.typeTag }.toMap[String, TypeTag[_]]
    DeriveScala.checkInputTypes(paramTypes)
    val t = ScalaScript.compileAndGetType(
      exprString, paramTypes, paramsToOption = false).returnType

    val s = namedScalars.map { case (k, v) => k -> v.typeTag }
    val e = exprString
    val op = t match {
      case _ if t =:= typeOf[String] => DeriveScalaScalar[String](e, s)
      case _ if t =:= typeOf[Double] => DeriveScalaScalar[Double](e, s)
      case _ => throw new AssertionError(s"Unsupported result type of expression $exprString: $t")
    }

    import Scripting._
    op(op.scalars, namedScalars.map(_._2.asInstanceOf[Scalar[Any]])).result
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

