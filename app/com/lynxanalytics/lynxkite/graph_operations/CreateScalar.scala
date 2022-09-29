// Operations for creating new scalars from nothing.
package com.lynxanalytics.lynxkite.graph_operations

import scala.reflect.runtime.universe._
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.controllers.UIStatus
import play.api.libs.json

object CreateScalar {
  class Output[T](implicit instance: MetaGraphOperationInstance, tt: TypeTag[T]) extends MagicOutput(instance) {
    val created = scalar[T]
  }
}
import CreateScalar._
abstract class CreateScalar[T] extends SparkOperation[NoInput, Output[T]] {
  def tt: TypeTag[T]
  val value: T
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, tt)
  def execute(
      inputDatas: DataSet,
      o: Output[T],
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    output(o.created, value)
  }
}

object CreateStringScalar extends OpFromJson {
  def fromJson(j: JsValue) = CreateStringScalar((j \ "value").as[String])
}
case class CreateStringScalar(value: String) extends CreateScalar[String] {
  @transient lazy val tt = typeTag[String]
  override def toJson = Json.obj("value" -> value)
}

object CreateUIStatusScalar extends OpFromJson {
  import com.lynxanalytics.lynxkite.controllers.UIStatusSerialization._
  def fromJson(j: JsValue) = CreateUIStatusScalar((j \ "value").as[UIStatus])
}
case class CreateUIStatusScalar(value: UIStatus) extends CreateScalar[UIStatus] {
  @transient lazy val tt = typeTag[UIStatus]
  import com.lynxanalytics.lynxkite.controllers.UIStatusSerialization._
  override def toJson = Json.obj("value" -> value)
}
