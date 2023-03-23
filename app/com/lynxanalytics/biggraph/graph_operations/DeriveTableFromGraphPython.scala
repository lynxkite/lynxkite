// Creates a table using Python executed on Sphynx, using a graph as the input.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import org.apache.spark
import DerivePython.Field
import DerivePython.Input
import DeriveTablePython.Output

object DeriveTableFromGraphPython extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveTableFromGraphPython(
      (j \ "code").as[String],
      (j \ "inputFields").as[List[Field]],
      (j \ "outputFields").as[List[Field]])
  }
}

import DeriveTableFromGraphPython._
case class DeriveTableFromGraphPython private[graph_operations] (
    code: String,
    inputFields: List[Field],
    outputFields: List[Field])
    extends TypedMetaGraphOp[Input, Output] {
  override def toJson = Json.obj(
    "code" -> code,
    "inputFields" -> inputFields,
    "outputFields" -> outputFields)
  override lazy val inputs = new Input(inputFields)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, outputFields)
}
