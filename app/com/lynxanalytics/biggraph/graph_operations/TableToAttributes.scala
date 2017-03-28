// Turns a Table into a VertexSet and its Attributes.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.sql.types
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.protection.Limitations
import com.lynxanalytics.biggraph.spark_util.SQLHelper

object TableToAttributes extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  def fromJson(j: JsValue) = TableToAttributes()

  def run(table: Table)(implicit m: MetaGraphManager): Output = {
    import Scripting._
    val op = TableToAttributes()
    op(op.t, table).result
  }

  private def toSymbol(field: types.StructField) = Symbol("imported_column_" + field.name)

  private def toNumberedLines(
    dataFrame: spark.sql.DataFrame, rc: RuntimeContext): AttributeRDD[Seq[Any]] = {
    val numRows = dataFrame.count()
    val maxRows = Limitations.maxImportedLines
    if (maxRows >= 0) {
      if (numRows > maxRows) {
        throw new AssertionError(
          s"Can't import $numRows lines as your licence only allows $maxRows.")
      }
    }
    val seqRDD = SQLHelper.toSeqRDD(dataFrame)
    val partitioner = rc.partitionerForNRows(numRows)
    import com.lynxanalytics.biggraph.spark_util.Implicits._
    seqRDD.randomNumbered(partitioner)
  }

  class Output(schema: types.StructType)(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    // Methods for listing the output entities for metagraph building purposes.
    private def attributeFromTypeTag[T: TypeTag](
      ids: => EntityContainer[VertexSet], name: scala.Symbol): EntityContainer[Attribute[T]] =
      vertexAttribute[T](ids, name)

    private def attributeFromField(
      ids: => EntityContainer[VertexSet],
      field: types.StructField): EntityContainer[Attribute[_]] = {
      attributeFromTypeTag(ids, toSymbol(field))(SQLHelper.typeTagFromDataType(field.dataType))
    }

    val ids = vertexSet
    val columns = schema.map {
      field => field.name -> attributeFromField(ids, field)
    }.toMap

    // Methods for populating this output instance with computed output RDDs.
    def populateOutput(
      rc: RuntimeContext,
      schema: types.StructType,
      dataFrame: spark.sql.DataFrame) {
      val entities = this.columns.values.map(_.entity)
      val entitiesByName = entities.map(e => (e.name, e): (scala.Symbol, Attribute[_])).toMap
      val inOrder = schema.map(f => entitiesByName(toSymbol(f)))

      rc.ioContext.writeAttributes(inOrder, toNumberedLines(dataFrame, rc))
    }
  }
}
import TableToAttributes._
case class TableToAttributes() extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    val schema = inputs.t.entity.schema
    new Output(schema)(instance)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val t = inputs.t.data
    assert(t.entity.schema == t.df.schema,
      s"Schema mismatch: ${t.entity.schema} != ${t.df.schema}")
    o.populateOutput(rc, t.df.schema, t.df)
  }
}
