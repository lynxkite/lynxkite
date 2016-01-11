// Creates a new attribute by evaluating a JavaScript expression over other attributes.
package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._

object DeriveJS {
  class Input(attrCount: Int, scalarCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val attrs = (0 until attrCount).map(i => vertexAttribute[JSValue](vs, Symbol("attr-" + i)))
    val scalars = (0 until scalarCount).map(i => scalar[JSValue](Symbol("scalar-" + i)))
  }
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance,
                           inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[T](inputs.vs.entity)
  }
  def negative(x: Attribute[Double])(implicit manager: MetaGraphManager): Attribute[Double] = {
    import Scripting._
    val op = DeriveJSDouble(JavaScript("-x"), Seq("x"))
    op(op.attrs, Seq(x).map(VertexAttributeToJSValue.run[Double])).result.attr
  }

  def deriveFromAttributes[T: TypeTag](
    exprString: String,
    namedAttributes: Seq[(String, Attribute[_])],
    vertexSet: VertexSet,
    namedScalars: Seq[(String, Scalar[_])] = Seq())(implicit manager: MetaGraphManager): Output[T] = {

    // Check name collision between scalars and attributes
    val common =
      namedAttributes.map(_._1).toSet & namedScalars.map(_._1).toSet
    assert(common.isEmpty, {
      val collisions = common.mkString(",")
      s"Identical scalar and attribute name: $collisions." +
        s" Please rename either the scalar or the attribute."
    })

    val js = JavaScript(exprString)

    // Good to go, let's prepare the attributes for DeriveJS.
    val jsValueAttributes =
      namedAttributes.map { case (_, attr) => VertexAttributeToJSValue.run(attr) }

    val jsValueScalars =
      namedScalars.map { case (_, sclr) => ScalarToJSValue.run(sclr) }

    val op: DeriveJS[T] =
      if (typeOf[T] =:= typeOf[String]) {
        DeriveJSString(js, namedAttributes.map(_._1), namedScalars.map(_._1)).asInstanceOf[DeriveJS[T]]
      } else if (typeOf[T] =:= typeOf[Double]) {
        DeriveJSDouble(js, namedAttributes.map(_._1), namedScalars.map(_._1)).asInstanceOf[DeriveJS[T]]
      } else ???

    val defaultAttributeValues =
      namedAttributes.map { case (_, attr) => JSValue.defaultValue(attr.typeTag).value }
    val defaultScalarValues =
      namedScalars.map { case (_, sc) => JSValue.defaultValue(sc.typeTag).value }
    op.validateJS[T](defaultAttributeValues, defaultScalarValues)

    import Scripting._
    op(op.vs, vertexSet)(op.attrs, jsValueAttributes)(op.scalars, jsValueScalars).result
  }
}
import DeriveJS._
abstract class DeriveJS[T](
  expr: JavaScript,
  attrNames: Seq[String],
  scalarNames: Seq[String])
    extends TypedMetaGraphOp[Input, Output[T]] {
  implicit def resultTypeTag: TypeTag[T]
  override val isHeavy = true
  @transient override lazy val inputs = new Input(attrNames.size, scalarNames.size)
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(resultTypeTag, instance, inputs)

  // Validate JS using default values for the types of the attributes.
  def validateJS[T: TypeTag](
    defaultAttributeValues: Seq[Any],
    defaultScalarValues: Seq[Any]): Unit = {
    val testNamedValues =
      (attrNames ++ scalarNames).zip(defaultAttributeValues ++ defaultScalarValues).toMap
    val result = expr.evaluate(testNamedValues, desiredClass)
    if (result != null) {
      convert(result)
    }
  }

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val joined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => new Array[JSValue](attrNames.size))
      inputs.attrs.zipWithIndex.foldLeft(noAttrs) {
        case (rdd, (attr, idx)) =>
          rdd.sortedJoin(attr.rdd).mapValues {
            case (attrs, attr) => {
              attrs(idx) = attr
              attrs
            }
          }
      }
    }

    val scalars = inputs.scalars.map { _.value }.toArray
    val allNames = attrNames ++ scalarNames

    val derived = joined.flatMapOptionalValues {
      case values =>
        val namedValues = allNames.zip(values ++ scalars).toMap.mapValues(_.value)
        // JavaScript's "undefined" is returned as a Java "null".
        Option(expr.evaluate(namedValues, desiredClass)).map(convert(_))
    }
    output(o.attr, derived)
  }

  protected val desiredClass: Class[_]
  protected def convert(v: Any): T
}

object DeriveJSString extends OpFromJson {
  private val scalarNamesParameter = NewParameter[Seq[String]]("scalarNames", Seq())
  def fromJson(j: JsValue) =
    DeriveJSString(JavaScript(
      (j \ "expr").as[String]),
      (j \ "attrNames").as[Seq[String]],
      scalarNamesParameter.fromJson(j))
}
case class DeriveJSString(
  expr: JavaScript,
  attrNames: Seq[String],
  scalarNames: Seq[String] = Seq())
    extends DeriveJS[String](expr, attrNames, scalarNames) {
  @transient lazy val resultTypeTag = typeTag[String]
  override def toJson = Json.obj(
    "expr" -> expr.expression,
    "attrNames" -> attrNames) ++
    DeriveJSString.scalarNamesParameter.toJson(scalarNames)
  val desiredClass = classOf[String]
  def convert(v: Any): String = v match {
    case v: String => v
    case _ => throw new AssertionError(s"$v of ${v.getClass} cannot be converted to String")
  }
}

object DeriveJSDouble extends OpFromJson {
  private val scalarNamesParameter = NewParameter[Seq[String]]("scalarNames", Seq())
  def fromJson(j: JsValue) =
    DeriveJSDouble(JavaScript(
      (j \ "expr").as[String]),
      (j \ "attrNames").as[Seq[String]],
      scalarNamesParameter.fromJson(j))
}
case class DeriveJSDouble(
  expr: JavaScript,
  attrNames: Seq[String],
  scalarNames: Seq[String] = Seq())
    extends DeriveJS[Double](expr, attrNames, scalarNames) {
  @transient lazy val resultTypeTag = typeTag[Double]
  override def toJson = Json.obj(
    "expr" -> expr.expression,
    "attrNames" -> attrNames) ++
    DeriveJSDouble.scalarNamesParameter.toJson(scalarNames)
  val desiredClass = classOf[java.lang.Double]
  def convert(v: Any): Double = v match {
    case v: Double => {
      assert(!v.isNaN(), s"$expr did not return a valid number")
      v
    }
    case _ => throw new AssertionError(s"$v of ${v.getClass} cannot be converted to Double")
  }
}
