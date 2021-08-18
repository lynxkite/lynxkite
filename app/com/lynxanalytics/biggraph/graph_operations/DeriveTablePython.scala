// Creates a new table using Python executed on Sphynx.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark
import DerivePython.Field

object DeriveTablePython extends OpFromJson {
  class Input extends MagicInputSignature {
    val df = table
  }
  class Output(implicit i: MetaGraphOperationInstance, fields: Seq[Field])
      extends MagicOutput(i) {
    val df = table(SQLHelper.dataFrameSchemaForTypes(fields.map(f => f.name -> f.tpe.typeTag)))
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveTablePython(
      (j \ "code").as[String],
      (j \ "outputFields").as[List[Field]])
  }
}

import DeriveTablePython._
case class DeriveTablePython private[graph_operations] (
    code: String,
    outputFields: List[Field])
    extends TypedMetaGraphOp[Input, Output]
    with UnorderedSphynxOperation {
  override def toJson = Json.obj(
    "code" -> code,
    "outputFields" -> outputFields)
  override lazy val inputs = new Input
  def outputMeta(i: MetaGraphOperationInstance) = new Output()(i, outputFields)
}
