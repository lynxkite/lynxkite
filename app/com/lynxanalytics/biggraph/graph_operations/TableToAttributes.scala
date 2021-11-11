// Turns a Table into a VertexSet and its Attributes.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.sql.types
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
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
      dataFrame: spark.sql.DataFrame,
      rc: RuntimeContext): AttributeRDD[Seq[Any]] = {
    val numRows = dataFrame.count()
    val seqRDD = SQLHelper.toSeqRDD(dataFrame)
    val partitioner = rc.partitionerForNRows(numRows)
    import com.lynxanalytics.biggraph.spark_util.Implicits._
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
    import com.lynxanalytics.biggraph.spark_util.Implicits._
    output(o.ids, df.select().rdd.map(_ => ()).randomNumbered(partitioner))
    for (f <- df.schema) {
      val attr = entitiesByName(toSymbol(f))
      val rdd = df.select(f.name).rdd.map(_.get(0))
      def outputRDD[T](attr: Attribute[T], rdd: AttributeRDD[Any]) =
        output(attr, rdd.asInstanceOf[AttributeRDD[T]])
      outputRDD(attr, rdd.randomNumbered(partitioner))
    }
  }
}
