// All operations related to converting between attribute types.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json
import play.api.libs.json.{ JsArray, JsValue }
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.controllers.UIStatus
import com.lynxanalytics.biggraph.controllers.UIStatusSerialization

// Dynamic values wrap various types into a combined type that we unwrap on FE side
// The combined type helps us joining arbitrary number of different typed attributes.
case class DynamicValue(
  string: String = "",
  defined: Boolean = true,
  double: Option[Double] = None,
  x: Option[Double] = None,
  y: Option[Double] = None)
object DynamicValue {
  val df = new java.text.DecimalFormat("#.#####")
  def converter[T: TypeTag]: (T => DynamicValue) = {
    if (typeOf[T] =:= typeOf[Double]) value => {
      val doubleValue = value.asInstanceOf[Double]
      if (doubleValue.isNaN) {
        DynamicValue(string = "undefined")
      } else if (doubleValue.isPosInfinity) {
        DynamicValue(string = "positive infinity")
      } else if (doubleValue.isNegInfinity) {
        DynamicValue(string = "negative infinity")
      } else {
        DynamicValue(double = Some(doubleValue), string = df.format(value))
      }
    }
    else if (typeOf[T] =:= typeOf[Long]) value =>
      DynamicValue(
        double = Some(value.asInstanceOf[Long].toDouble), string = value.toString)
    else if (typeOf[T] =:= typeOf[String]) value =>
      DynamicValue(string = value.asInstanceOf[String])
    else if (typeOf[T] =:= typeOf[(Double, Double)]) value => {
      val tuple = value.asInstanceOf[(Double, Double)]
      DynamicValue(string = value.toString, x = Some(tuple._1), y = Some(tuple._2))
    }
    else if (typeOf[T] <:< typeOf[Seq[Any]]) value => {
      val seq = value.asInstanceOf[Seq[Any]]
      DynamicValue(string = seq.mkString(", "))
    }
    else if (typeOf[T] <:< typeOf[Set[_]]) value => {
      val set = value.asInstanceOf[Set[Any]]
      DynamicValue(string = set.toSeq.map(_.toString).sorted.mkString(", "))
    }
    else if (typeOf[T] =:= typeOf[UIStatus]) value => {
      import UIStatusSerialization._
      val uiStatus = value.asInstanceOf[UIStatus]
      DynamicValue(string = json.Json.prettyPrint(json.Json.toJson(uiStatus)))
    }
    else value =>
      DynamicValue(string = value.toString)
  }
  def convert[T: TypeTag](value: T): DynamicValue = {
    val c = converter[T]
    c(value)
  }
}

object VertexAttributeToString extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[String](inputs.vs.entity)
  }
  def run[T](attr: Attribute[T])(
    implicit manager: MetaGraphManager): Attribute[String] = {

    import Scripting._
    val op = VertexAttributeToString[T]()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = VertexAttributeToString()
}
case class VertexAttributeToString[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToString.Output[T]] {
  import VertexAttributeToString._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.toString))
  }
}

object VertexAttributeToDouble extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[String])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }
  def run(attr: Attribute[String])(
    implicit manager: MetaGraphManager): Attribute[Double] = {

    import Scripting._
    val op = VertexAttributeToDouble()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = VertexAttributeToDouble()
}
case class VertexAttributeToDouble()
    extends TypedMetaGraphOp[VertexAttributeInput[String], VertexAttributeToDouble.Output] {
  import VertexAttributeToDouble._
  @transient override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.flatMapOptionalValues(str =>
      if (str.nonEmpty) Some(str.toDouble) else None))
  }
}

object LongAttributeToDouble extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[Long])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }
  def run(attr: Attribute[Long])(
    implicit manager: MetaGraphManager): Attribute[Double] = {

    import Scripting._
    val op = LongAttributeToDouble()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = LongAttributeToDouble()
}
case class LongAttributeToDouble()
    extends TypedMetaGraphOp[VertexAttributeInput[Long], LongAttributeToDouble.Output] {
  import LongAttributeToDouble._
  @transient override lazy val inputs = new VertexAttributeInput[Long]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.mapValues(_.toDouble))
  }
}

object DoubleAttributeToLong extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[Double])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Long](inputs.vs.entity)
  }
  def run(attr: Attribute[Double])(
    implicit manager: MetaGraphManager): Attribute[Long] = {

    import Scripting._
    val op = DoubleAttributeToLong()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = DoubleAttributeToLong()
}
case class DoubleAttributeToLong()
    extends TypedMetaGraphOp[VertexAttributeInput[Double], DoubleAttributeToLong.Output] {
  import DoubleAttributeToLong._
  @transient override lazy val inputs = new VertexAttributeInput[Double]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.mapValues(_.round))
  }
}

object IntAttributeToLong extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[Int])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Long](inputs.vs.entity)
  }
  def run(attr: Attribute[Int])(
    implicit manager: MetaGraphManager): Attribute[Long] = {

    import Scripting._
    val op = IntAttributeToLong()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = IntAttributeToLong()
}
case class IntAttributeToLong()
    extends TypedMetaGraphOp[VertexAttributeInput[Int], IntAttributeToLong.Output] {
  import IntAttributeToLong._
  @transient override lazy val inputs = new VertexAttributeInput[Int]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.mapValues(_.toLong))
  }
}

object VertexAttributeToDynamicValue extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[DynamicValue](inputs.vs.entity)
  }
  def run[T](attr: Attribute[T])(
    implicit manager: MetaGraphManager): Attribute[DynamicValue] = {

    import Scripting._
    val op = VertexAttributeToDynamicValue[T]()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = VertexAttributeToDynamicValue()
}
case class VertexAttributeToDynamicValue[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToDynamicValue.Output[T]] {
  import VertexAttributeToDynamicValue._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    implicit val tt = inputs.attr.data.typeTag
    val attr = inputs.attr.rdd
    val converter = DynamicValue.converter[T]
    output(o.attr, attr.mapValues(converter(_)))
  }
}

object AttributeCast {
  class Output[From, To: TypeTag](
    implicit instance: MetaGraphOperationInstance, inputs: VertexAttributeInput[From])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[To](inputs.vs.entity)
  }
}
abstract class AttributeCast[From, To]()
    extends TypedMetaGraphOp[VertexAttributeInput[From], AttributeCast.Output[From, To]] {
  import AttributeCast._
  @transient override lazy val inputs = new VertexAttributeInput[From]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[From, To]()(tt, instance, inputs)
  def tt: TypeTag[To]

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.asInstanceOf[To]))
  }
}

object AttributeVectorToAny extends OpFromJson {
  def fromJson(j: JsValue) = AttributeVectorToAny()
}
case class AttributeVectorToAny[From]() extends AttributeCast[Vector[From], Vector[Any]] {
  @transient lazy val tt = typeTag[Vector[Any]]
}

case class JSValue(value: Any)

object JSValue {
  def converterForType(t: Type): (Any => Any) = {
    if (t <:< typeOf[AnyVal]) {
      if (t =:= typeOf[Long]) {
        throw new AssertionError(
          "The Long type is not supported in JavaScript due to language limitations." +
            " Please convert to Double or String first.")
      }
      if (t =:= typeOf[Unit]) {
        throw new AssertionError("The Unit type is not supported in JavaScript.")
      }
      primitive => primitive
    } else if (t =:= typeOf[String]) {
      string => string
    } else if (t <:< typeOf[Iterable[_]]) {
      val TypeRef(_, _, iterableParams) = t.baseType(typeOf[Iterable[_]].typeSymbol)
      assert(iterableParams.size == 1, "How can a iterable have !=1 params?")
      val dataType = iterableParams(0)
      if (dataType =:= typeOf[Double]) {
        iterable => iterable.asInstanceOf[Iterable[Double]].toArray
      } else if (dataType =:= typeOf[Short]) {
        iterable => iterable.asInstanceOf[Iterable[Short]].toArray
      } else if (dataType =:= typeOf[Int]) {
        iterable => iterable.asInstanceOf[Iterable[Int]].toArray
      } else if (dataType =:= typeOf[Float]) {
        iterable => iterable.asInstanceOf[Iterable[Float]].toArray
      } else if (dataType =:= typeOf[Char]) {
        iterable => iterable.asInstanceOf[Iterable[Char]].toArray
      } else if (dataType =:= typeOf[Boolean]) {
        iterable => iterable.asInstanceOf[Iterable[Boolean]].toArray
      } else {
        val elementConverter = converterForType(dataType)
        iterable => {
          val converted = iterable.asInstanceOf[Iterable[Any]].map(elementConverter(_))
          converted.toArray
        }
      }
    } else {
      throw new AssertionError(s"Type: $t is not supported.")
    }
  }

  def converter[T: TypeTag]: (T => JSValue) = {
    val innerConverter = converterForType(typeOf[T])
    value => JSValue(innerConverter(value))
  }

  def convert[T: TypeTag](value: T): JSValue = {
    val c = converter[T]
    c(value)
  }

  def defaultJavaValue[T: TypeTag]: T = {
    val res =
      if (typeOf[T] =:= typeOf[Byte]) 0.toByte
      else if (typeOf[T] =:= typeOf[Short]) 0.toShort
      else if (typeOf[T] =:= typeOf[Int]) 0
      else if (typeOf[T] =:= typeOf[Float]) 0.toFloat
      else if (typeOf[T] =:= typeOf[Double]) 0.0
      else if (typeOf[T] =:= typeOf[Char]) 'a'
      else if (typeOf[T] =:= typeOf[Boolean]) false
      else if (typeOf[T] =:= typeOf[String]) ""
      else if (typeOf[T] <:< typeOf[Iterable[_]]) Iterable()
      else throw new AssertionError(s"Type ${typeOf[T]} is not supported as input for Javascript")
    res.asInstanceOf[T]
  }
  def defaultValue[T: TypeTag]: JSValue = {
    convert(defaultJavaValue)
  }
}

object VertexAttributeToJSValue extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[JSValue](inputs.vs.entity)
  }
  def run[T](attr: Attribute[T])(
    implicit manager: MetaGraphManager): Attribute[JSValue] = {

    import Scripting._
    val op = VertexAttributeToJSValue[T]()
    op(op.attr, attr).result.attr
  }
  def seq(attrs: Attribute[_]*)(
    implicit manager: MetaGraphManager): Seq[Attribute[JSValue]] = {

    attrs.map(run(_))
  }
  def fromJson(j: JsValue) = VertexAttributeToJSValue()
}
case class VertexAttributeToJSValue[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToJSValue.Output[T]] {
  import VertexAttributeToJSValue._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    implicit val tt = inputs.attr.data.typeTag
    val attr = inputs.attr.rdd
    val converter = JSValue.converter[T]
    output(o.attr, attr.mapValues(converter(_)))
  }
}

object ScalarToJSValue extends OpFromJson {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: ScalarInput[T])
      extends MagicOutput(instance) {
    val sc = scalar[JSValue]
  }
  def run[T](scalar: Scalar[T])(
    implicit manager: MetaGraphManager): Scalar[JSValue] = {

    import Scripting._
    val op = ScalarToJSValue[T]()
    op(op.sc, scalar).result.sc
  }
  def seq(scalars: Scalar[_]*)(
    implicit manager: MetaGraphManager): Seq[Scalar[JSValue]] = {

    scalars.map(run(_))
  }
  def fromJson(j: JsValue) = ScalarToJSValue()
}
case class ScalarToJSValue[T]()
    extends TypedMetaGraphOp[ScalarInput[T], ScalarToJSValue.Output[T]] {
  import ScalarToJSValue._
  @transient override lazy val inputs = new ScalarInput[T]
  override val neverSerialize = true
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.sc.data.typeTag
    val scalar = inputs.sc.value
    output(o.sc, JSValue.convert(scalar))
  }
}

object TypeTagToFormat {

  implicit val formatDynamicFormat = json.Json.format[DynamicValue]
  implicit val formatEdge = json.Json.format[Edge]

  implicit object ToJsonFormat extends json.Format[ToJson] {
    def writes(t: ToJson): JsValue = {
      t.toTypedJson
    }
    def reads(j: json.JsValue): json.JsResult[ToJson] = {
      json.JsSuccess(TypedJson.read(j))
    }
  }

  def optionToFormat[T](t: TypeTag[T]): json.Format[Option[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Option[T]]]
  }

  def pairToFormat[A, B](a: TypeTag[A], b: TypeTag[B]): json.Format[(A, B)] = {
    implicit val innerFormat1 = typeTagToFormat(a)
    implicit val innerFormat2 = typeTagToFormat(b)
    new PairFormat[A, B]
  }

  class PairFormat[A: json.Format, B: json.Format] extends json.Format[(A, B)] {
    def reads(j: json.JsValue): json.JsSuccess[(A, B)] = {
      json.JsSuccess(((j \ "first").as[A], (j \ "second").as[B]))
    }
    def writes(v: (A, B)): json.JsValue = {
      json.Json.obj(
        "first" -> v._1,
        "second" -> v._2
      )
    }
  }

  class MapFormat[A: json.Format, B: json.Format] extends json.Format[Map[A, B]] {
    def reads(jv: json.JsValue): json.JsSuccess[Map[A, B]] = {
      val extracted = jv match {
        case JsArray(arr) => arr.map {
          element => (element \ "key").as[A] -> (element \ "val").as[B]
        }
        case _ => ???
      }
      json.JsSuccess(extracted.toMap)
    }
    def writes(v: Map[A, B]): json.JsValue = {
      val sss = v.map {
        case (key, value) =>
          json.Json.obj(
            "key" -> json.Json.toJson(key),
            "val" -> json.Json.toJson(value)
          )
      }.toSeq
      JsArray(sss)
    }
  }

  def mapToFormat[A, B](a: TypeTag[A], b: TypeTag[B]): json.Format[Map[A, B]] = {
    implicit val innerFormat1 = typeTagToFormat(a)
    implicit val innerFormat2 = typeTagToFormat(b)
    new MapFormat[A, B]
  }

  def seqToFormat[T](t: TypeTag[T]): json.Format[Seq[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Seq[T]]]
  }

  def setToFormat[T](t: TypeTag[T]): json.Format[Set[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Set[T]]]
  }

  def typeTagToFormat[T](tag: TypeTag[T]): json.Format[T] = {

    case class TypeWrapper(t: Type) {
      def isASimple(other: Type) = t =:= other
      def isA(other: Type) = t.typeConstructor =:= other.typeConstructor
      def isASubtypeOf(other: Type) = t <:< other
    }

    val t = TypeWrapper(tag.tpe)
    val fmt = {
      if (t isASimple typeOf[String]) implicitly[json.Format[String]]
      else if (t isASimple typeOf[Double]) implicitly[json.Format[Double]]
      else if (t isASimple typeOf[Long]) implicitly[json.Format[Long]]
      else if (t isASimple typeOf[Boolean]) implicitly[json.Format[Boolean]]
      else if (t isASimple typeOf[Int]) implicitly[json.Format[Int]]
      else if (t isASimple typeOf[Float]) implicitly[json.Format[Float]]
      else if (t isASimple typeOf[DynamicValue]) implicitly[json.Format[DynamicValue]]
      else if (t isASimple typeOf[Edge]) implicitly[json.Format[Edge]]
      else if (t isASubtypeOf typeOf[ToJson]) ToJsonFormat
      else if (t isA typeOf[Option[_]]) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        optionToFormat(innerType)
      } else if (t isA typeOf[(_, _)]) {
        val firstInnerType = TypeTagUtil.typeArgs(tag).head
        val secondInnerType = TypeTagUtil.typeArgs(tag).drop(1).head
        pairToFormat(firstInnerType, secondInnerType)
      } else if (t isA typeOf[Seq[_]]) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        seqToFormat(innerType)
      } else if (t isA typeOf[Set[_]]) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        setToFormat(innerType)
      } else if (t isA typeOf[Map[_, _]]) {
        val firstInnerType = TypeTagUtil.typeArgs(tag).head
        val secondInnerType = TypeTagUtil.typeArgs(tag).drop(1).head
        mapToFormat(firstInnerType, secondInnerType)
      } else {
        throw new AssertionError(s"Unsupported type: $t")
      }
    }.asInstanceOf[json.Format[T]]
    fmt
  }
}
