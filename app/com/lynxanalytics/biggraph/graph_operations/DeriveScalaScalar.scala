// Creates a new scalar by evaluating a Scala expression over other scalars.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.sandbox.ScalaScript

object DeriveScalaScalar extends OpFromJson {
  class Input(scalarCount: Int) extends MagicInputSignature {
    val scalars = (0 until scalarCount).map(i => anyScalar(Symbol("scalar-" + i)))
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
    val t = ScalaScript.compileAndGetType(
      exprString, paramTypes, paramsToOption = false).returnType

    val s = namedScalars.map(_._1)
    val e = exprString
    val op = t match {
      case _ if t =:= typeOf[String] => DeriveScalaScalar[String](e, s)
      case _ if t =:= typeOf[Double] => DeriveScalaScalar[Double](e, s)
      case _ => throw new AssertionError(s"Unsupported result type of expression $exprString: $t")
    }

    import Scripting._
    op(op.scalars, namedScalars.map(_._2)).result
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    implicit val tt = SerializableType.fromJson(j \ "type").typeTag
    DeriveScalaScalar(
      (j \ "expr").as[String],
      (j \ "scalarNames").as[List[String]].toSeq)
  }
}
import DeriveScalaScalar._
case class DeriveScalaScalar[T: TypeTag](
  expr: String,
  scalarNames: Seq[String])
    extends TypedMetaGraphOp[Input, Output[T]] {

  def tt = typeTag[T]
  def st = SerializableType(tt)
  implicit def ct = RuntimeSafeCastable.classTagFromTypeTag(tt)
  override def toJson = Json.obj(
    "type" -> st.toJson,
    "expr" -> expr,
    "scalarNames" -> scalarNames)

  override val isHeavy = true
  @transient override lazy val inputs = new Input(scalarNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(tt, instance)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val scalars = inputs.scalars.map(_.value)
    val paramTypes =
      scalarNames.zip(inputs.scalars).map { case (k, v) => k -> v.data.typeTag }
        .toMap[String, TypeTag[_]]

    val t = ScalaScript.compileAndGetType(expr, paramTypes, paramsToOption = false)
    assert(t.returnType =:= tt.tpe,
      s"Scala script returns wrong type: expected ${tt.tpe} but got ${t.returnType} instead.")

    val evaluator = ScalaScript.compileAndGetEvaluator(expr, paramTypes, paramsToOption = false)
    val namedValues = scalarNames.zip(scalars).toMap
    val result = evaluator.evaluate(namedValues)
    assert(Option(result).nonEmpty, s"Scala script $expr returned null.")
    output(o.sc, result.asInstanceOf[T])
  }
}

