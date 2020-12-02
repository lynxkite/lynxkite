// All operations related to converting between attribute types.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import scala.reflect.runtime.universe._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.controllers.UIStatus
import com.lynxanalytics.biggraph.controllers.UIStatusSerialization
import com.lynxanalytics.biggraph.model

// Dynamic values wrap various types into a combined type that we unwrap on FE side
// The combined type helps us joining arbitrary number of different typed attributes.
case class DynamicValue(
    string: String = "",
    defined: Boolean = true,
    double: Option[Double] = None,
    vector: List[Double] = Nil)
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
    else if (typeOf[T] =:= typeOf[Int]) value =>
      DynamicValue(
        double = Some(value.asInstanceOf[Int].toDouble), string = value.toString)
    else if (typeOf[T] =:= typeOf[String]) value =>
      DynamicValue(string = value.asInstanceOf[String])
    else if (typeOf[T] <:< typeOf[Seq[_]]) {
      seqConverter(TypeTagUtil.typeArgs(typeTag[T]).head).asInstanceOf[T => DynamicValue]
    } else if (typeOf[T] <:< typeOf[Set[_]]) {
      setConverter(TypeTagUtil.typeArgs(typeTag[T]).head).asInstanceOf[T => DynamicValue]
    } else if (typeOf[T] =:= typeOf[model.Model]) value => {
      val m = value.asInstanceOf[model.Model]
      if (m.labelName.isDefined) {
        DynamicValue(string = s"${m.method} model predicting ${m.labelName.get}")
      } else {
        DynamicValue(string = s"${m.method} unsupervised learning model")
      }

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

  def seqConverter[T](tt: TypeTag[_]): (Seq[T] => DynamicValue) = {
    val innerConverter = converter(tt.asInstanceOf[TypeTag[T]])
    if (tt.tpe =:= typeOf[Double]) {
      seq =>
        DynamicValue(
          string = seq.map(e => innerConverter(e).string).mkString(", "),
          vector = seq.asInstanceOf[Seq[Double]].toList)
    } else {
      seq => DynamicValue(string = seq.map(e => innerConverter(e).string).mkString(", "))
    }
  }

  def setConverter[T](tt: TypeTag[_]): (Set[T] => DynamicValue) = {
    val innerConverter = converter(tt.asInstanceOf[TypeTag[T]])
    if (tt.tpe =:= typeOf[Double]) {
      set =>
        DynamicValue(
          string = set.toSeq.map(e => innerConverter(e).string).sorted.mkString(", "),
          vector = set.asInstanceOf[Set[Double]].toList)
    } else {
      set => DynamicValue(string = set.toSeq.map(e => innerConverter(e).string).sorted.mkString(", "))
    }
  }
}

trait AttributeConverter[From, To] extends OpFromJson {
  def newOp: AttributeConverterOp[From, To]
  def run(attr: Attribute[From])(implicit manager: MetaGraphManager): Attribute[To] = {
    import Scripting._
    val op = newOp
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = newOp
}
abstract class AttributeConverterOp[From, To: TypeTag]()
  extends SparkOperation[VertexAttributeInput[From], AttributeOutput[To]] {
  @transient override lazy val inputs = new VertexAttributeInput[From]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[To](inputs.vs.entity)
  }
  def execute(inputDatas: DataSet, o: AttributeOutput[To], output: OutputBuilder, rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, convert(inputs.attr.rdd))
  }
  def convert(rdd: AttributeRDD[From]): AttributeRDD[To]
}

object VertexAttributeToString extends OpFromJson {
  def run[T](attr: Attribute[T])(implicit manager: MetaGraphManager): Attribute[String] = {
    import Scripting._
    val op = VertexAttributeToString[T]()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = VertexAttributeToString()
}
case class VertexAttributeToString[T]() extends AttributeConverterOp[T, String] {
  def convert(rdd: AttributeRDD[T]): AttributeRDD[String] = ??? // Unused.
  override def execute(inputDatas: DataSet, o: AttributeOutput[String], output: OutputBuilder, rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    if (inputs.attr.data.is[Double]) {
      // Double.toString always adds a ".0" at the end. ("1981.0")
      // We avoid that using DecimalFormat.
      val df = new java.text.DecimalFormat(
        "0", java.text.DecimalFormatSymbols.getInstance(java.util.Locale.ENGLISH))
      df.setMaximumFractionDigits(10)
      output(o.attr, inputs.attr.rdd.mapValues(df.format))
    } else {
      output(o.attr, inputs.attr.rdd.mapValues(_.toString))
    }
  }
}

object VertexAttributeToDouble extends AttributeConverter[String, Double] {
  def newOp = VertexAttributeToDouble()
}
case class VertexAttributeToDouble() extends AttributeConverterOp[String, Double] {
  def convert(rdd: AttributeRDD[String]): AttributeRDD[Double] = {
    rdd.flatMapOptionalValues(str => if (str.nonEmpty) Some(str.toDouble) else None)
  }
}

object LongAttributeToDouble extends AttributeConverter[Long, Double] {
  def newOp = LongAttributeToDouble()
}
case class LongAttributeToDouble() extends AttributeConverterOp[Long, Double] {
  def convert(rdd: AttributeRDD[Long]): AttributeRDD[Double] = rdd.mapValues(_.toDouble)
}

object DoubleAttributeToLong extends AttributeConverter[Double, Long] {
  def newOp = DoubleAttributeToLong()
}
case class DoubleAttributeToLong() extends AttributeConverterOp[Double, Long] {
  def convert(rdd: AttributeRDD[Double]): AttributeRDD[Long] = rdd.mapValues(_.round)
}

object IntAttributeToLong extends AttributeConverter[Int, Long] {
  def newOp = IntAttributeToLong()
}
case class IntAttributeToLong() extends AttributeConverterOp[Int, Long] {
  def convert(rdd: AttributeRDD[Int]): AttributeRDD[Long] = rdd.mapValues(_.toLong)
}

object IntAttributeToDouble extends AttributeConverter[Int, Double] {
  def newOp = IntAttributeToDouble()
}
case class IntAttributeToDouble() extends AttributeConverterOp[Int, Double] {
  def convert(rdd: AttributeRDD[Int]): AttributeRDD[Double] = rdd.mapValues(_.toDouble)
}

object FloatAttributeToDouble extends AttributeConverter[Float, Double] {
  def newOp = FloatAttributeToDouble()
}
case class FloatAttributeToDouble() extends AttributeConverterOp[Float, Double] {
  def convert(rdd: AttributeRDD[Float]): AttributeRDD[Double] = rdd.mapValues(_.toDouble)
}

object BigDecimalAttributeToDouble extends AttributeConverter[java.math.BigDecimal, Double] {
  def newOp = BigDecimalAttributeToDouble()
}
case class BigDecimalAttributeToDouble() extends AttributeConverterOp[java.math.BigDecimal, Double] {
  def convert(rdd: AttributeRDD[java.math.BigDecimal]): AttributeRDD[Double] = rdd.mapValues(_.doubleValue)
}

object VertexAttributeToDynamicValue extends OpFromJson {
  class Output[T](implicit
      instance: MetaGraphOperationInstance,
      inputs: VertexAttributeInput[T])
    extends MagicOutput(instance) {
    val attr = vertexAttribute[DynamicValue](inputs.vs.entity)
  }
  def run[T](attr: Attribute[T])(
    implicit
    manager: MetaGraphManager): Attribute[DynamicValue] = {

    import Scripting._
    val op = VertexAttributeToDynamicValue[T]()
    op(op.attr, attr).result.attr
  }
  def fromJson(j: JsValue) = VertexAttributeToDynamicValue()
}
case class VertexAttributeToDynamicValue[T]()
  extends SparkOperation[VertexAttributeInput[T], VertexAttributeToDynamicValue.Output[T]] {
  import VertexAttributeToDynamicValue._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(
    inputDatas: DataSet,
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

object AttributeVectorToAny extends OpFromJson {
  def fromJson(j: JsValue) = AttributeVectorToAny()
}
case class AttributeVectorToAny[From]() extends AttributeConverterOp[Vector[From], Vector[Any]] {
  def convert(rdd: AttributeRDD[Vector[From]]): AttributeRDD[Vector[Any]] = ??? // Unused.
  override def execute(inputDatas: DataSet, o: AttributeOutput[Vector[Any]], output: OutputBuilder, rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.asInstanceOf[Vector[Any]]))
  }
}
