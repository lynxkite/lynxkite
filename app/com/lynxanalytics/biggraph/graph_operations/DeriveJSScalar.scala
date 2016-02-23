// Creates a new scalar by evaluating a JavaScript expression over other scalars.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._

object DeriveJSScalar {
  class Input(scalarCount: Int) extends MagicInputSignature {
    val scalars = (0 until scalarCount).map(i => scalar[JSValue](Symbol("scalar-" + i)))
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val sc = scalar[T]
  }

  def deriveFromScalars[T: TypeTag](
    exprString: String,
    namedScalars: Seq[(String, Scalar[_])])(implicit manager: MetaGraphManager): Output[T] = {
    val js = JavaScript(exprString)
    val jsValueScalars =
      namedScalars.map { case (_, sclr) => ScalarToJSValue.run(sclr) }
    val op: DeriveJSScalar[T] =
      if (typeOf[T] =:= typeOf[String]) {
        DeriveJSScalarString(js, namedScalars.map(_._1)).asInstanceOf[DeriveJSScalar[T]]
      } else if (typeOf[T] =:= typeOf[Double]) {
        DeriveJSScalarDouble(js, namedScalars.map(_._1)).asInstanceOf[DeriveJSScalar[T]]
      } else ???
    val defaultScalarValues =
      namedScalars.map { case (_, sc) => JSValue.defaultValue(sc.typeTag).value }
    op.validateJS[T](defaultScalarValues)

    import Scripting._
    op(op.scalars, jsValueScalars).result
  }
}
import DeriveJSScalar._
abstract class DeriveJSScalar[T](
  expr: JavaScript,
  scalarNames: Seq[String])
    extends TypedMetaGraphOp[Input, Output[T]] {
  implicit def resultTypeTag: TypeTag[T]
  override val isHeavy = true
  @transient override lazy val inputs = new Input(scalarNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(resultTypeTag, instance)

  // Validate JS using default values for the types of the scalars.
  def validateJS[T: TypeTag](
    defaultScalarValues: Seq[Any]): Unit = {
    val testNamedValues = scalarNames.zip(defaultScalarValues).toMap
    evaluate(testNamedValues)
  }

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val scalars = inputs.scalars.map(_.value.value)
    val bindings = scalarNames.zip(scalars).toMap
    val result = evaluate(bindings)
    assert(!result.isEmpty, s"${expr.contextString(bindings)} returned undefined for scalar")
    val derived = check(
      result.get,
      expr.contextString(bindings))
    output(o.sc, derived)
  }

  protected def evaluate(mapping: Map[String, Any]): Option[T]
  protected def check(
    v: T, // The value to convert.
    context: => String): T // The context of the conversion for detailed error messages.
}

object DeriveJSScalarString extends OpFromJson {
  def fromJson(j: JsValue) =
    DeriveJSScalarString(JavaScript(
      (j \ "expr").as[String]),
      (j \ "scalarNames").as[Seq[String]])
}
case class DeriveJSScalarString(
  expr: JavaScript,
  scalarNames: Seq[String] = Seq())
    extends DeriveJSScalar[String](expr, scalarNames) {
  @transient lazy val resultTypeTag = typeTag[String]
  override def toJson = Json.obj(
    "expr" -> expr.expression,
    "scalarNames" -> scalarNames)
  def check(v: String, context: => String): String = v
  def evaluate(mapping: Map[String, Any]): Option[String] = {
    expr.evaluator.evaluateString(mapping)
  }
}

object DeriveJSScalarDouble extends OpFromJson {
  private val scalarNamesParameter = NewParameter[Seq[String]]("scalarNames", Seq())
  def fromJson(j: JsValue) =
    DeriveJSScalarDouble(JavaScript(
      (j \ "expr").as[String]),
      (j \ "scalarNames").as[Seq[String]])
}
case class DeriveJSScalarDouble(
  expr: JavaScript,
  scalarNames: Seq[String] = Seq())
    extends DeriveJSScalar[Double](expr, scalarNames) {
  @transient lazy val resultTypeTag = typeTag[Double]
  override def toJson = Json.obj(
    "expr" -> expr.expression,
    "scalarNames" -> scalarNames)
  def check(v: Double, context: => String): Double = {
    assert(!v.isNaN() && !v.isInfinite(), s"$context did not return a valid number: $v")
    v
  }
  def evaluate(mapping: Map[String, Any]): Option[Double] = {
    expr.evaluator.evaluateDouble(mapping)
  }
}
