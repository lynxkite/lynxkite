package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._
import com.lynxanalytics.biggraph.graph_api._

object CreateScalar {
  class Output[T: TypeTag](implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val value = scalar[T]
  }
}
import CreateScalar._
abstract class CreateScalar[T] extends TypedMetaGraphOp[NoInput, Output[T]] {
  implicit def tt: TypeTag[T]
  val value: T
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = { implicit val i = instance; new Output[T]() }
  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    output(o.value, value)
  }
}

case class CreateStringScalar(value: String) extends CreateScalar[String] {
  @transient lazy val tt = typeTag[String]
}
