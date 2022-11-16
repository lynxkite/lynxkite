// Creates an HTML output using Python executed on Sphynx.
// For security, this output should not be freely controlled by the user.
// Instead it's used for returning plots encoded in HTML.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import DerivePython._

import org.apache.spark

object DeriveHTMLPython extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveHTMLPython(
      (j \ "code").as[String],
      (j \ "mode").as[String],
      (j \ "inputFields").as[List[Field]])
  }
}

case class DeriveHTMLPython private[graph_operations] (
    code: String,
    mode: String,
    inputFields: List[Field])
    extends TypedMetaGraphOp[Input, ScalarOutput[String]] {
  override def toJson = Json.obj(
    "code" -> code,
    "mode" -> mode,
    "inputFields" -> inputFields)
  override lazy val inputs = new Input(inputFields)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new ScalarOutput[String]
  }
}
