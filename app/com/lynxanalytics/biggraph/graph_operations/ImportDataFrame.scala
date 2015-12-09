// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types

object ImportDataFrame extends OpFromJson {
  type SomeAttribute = Attribute[_]
  class Output(schema: types.StructType)(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    def toSymbol(field: String) = Symbol("imported_column_" + field)
    def attributeFromField(ids: => EntityContainer[VertexSet], field: types.StructField): EntityContainer[SomeAttribute] = {
      field.dataType match {
        // TODO: extend this, or find a better way to get to a TypeTag.
        case _: types.DoubleType => vertexAttribute[Double](ids, toSymbol(field.name))
        case _: types.LongType => vertexAttribute[Long](ids, toSymbol(field.name))
        case _: types.StringType => vertexAttribute[String](ids, toSymbol(field.name))
      }
    }

    val ids = vertexSet
    val columns = schema.iterator.map {
      field => field.name -> attributeFromField(ids, field)
    }.toMap
  }
  def fromJson(j: JsValue) = new ImportDataFrame(
    types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType],
    None)

  def apply(inputFrame: DataFrame) = new ImportDataFrame(inputFrame.schema, Some(inputFrame))
}

class ImportDataFrame private (schema: types.StructType, inputFrame: Option[DataFrame])
    extends TypedMetaGraphOp[NoInput, ImportDataFrame.Output] {
  import ImportDataFrame._
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output(schema)(instance)
  override def toJson = Json.obj("schema" -> schema.prettyJson)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val sc = rc.sparkContext

    /*val entities = o.attrs.values.map(_.entity)
    val entitiesByName = entities.map(e => e.name -> e).toMap
    val inOrder = input.fields.map(f => entitiesByName(ImportCommon.toSymbol(f)))

    val lines = numberedLines(rc, input)
    rc.ioContext.writeAttributes(inOrder, lines)*/
  }
}
