// Turns a Table into a VertexSet and its Attributes.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark
import org.apache.spark.sql.types
import scala.reflect.runtime.universe._

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.SQLHelper

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
      dataFrame: spark.sql.DataFrame,
      rc: RuntimeContext): AttributeRDD[Seq[Any]] = {
    val numRows = dataFrame.count()
    val seqRDD = SQLHelper.toSeqRDD(dataFrame)
    val partitioner = rc.partitionerForNRows(numRows)
    import com.lynxanalytics.lynxkite.spark_util.Implicits._
    seqRDD.randomNumbered(partitioner)
  }

  class Output(schema: types.StructType)(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    // Methods for listing the output entities for metagraph building purposes.
    private def attributeFromTypeTag[T: TypeTag](
        ids: => EntityContainer[VertexSet],
        name: scala.Symbol): EntityContainer[Attribute[T]] =
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
  }
}
import TableToAttributes._
case class TableToAttributes() extends SparkOperation[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val inst = instance
    val schema = inputs.t.entity.schema
    new Output(schema)(instance)
  }

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val t = inputs.t.data
    val df = t.df
    SQLHelper.assertTableHasCorrectSchema(t.entity, df.schema)
    val numRows = df.count
    val partitioner = rc.partitionerForNRows(numRows)
    val entities = o.columns.values.map(_.entity)
    val entitiesByName = entities.map(e => (e.name, e): (scala.Symbol, Attribute[_])).toMap
    import com.lynxanalytics.lynxkite.spark_util.Implicits._
    // We rely on "df" originating from a Parquet file. As it's a column oriented format with
    // predicate push down, we can read each attribute in a separate pass without reading the
    // whole dataset multiple times.
    //
    // We select empty rows and assign vertex IDs to each row. That's the VertexSet.
    output(o.ids, df.select().rdd.map(_ => ()).randomNumbered(partitioner))
    // Now we have to assign the same IDs to the attributes. randomNumbered is deterministic.
    for (f <- df.schema) {
      val attr = entitiesByName(toSymbol(f))
      // The only way to reliably select a column (which may have special characters) is like this:
      val rdd = df.select("`" + f.name + "`").rdd
      def outputRDD[T](attr: Attribute[T], rdd: AttributeRDD[spark.sql.Row]) =
        output(attr, rdd.filter(!_._2.isNullAt(0)).mapValues(_.get(0).asInstanceOf[T]))
      // Missing values are not written out, but they are still present in the DataFrame.
      // We have to assign the IDs before filtering out the nulls.
      outputRDD(attr, rdd.randomNumbered(partitioner))
    }
  }
}
