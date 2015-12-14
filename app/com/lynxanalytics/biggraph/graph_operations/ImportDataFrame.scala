// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.protection.Limitations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

object ImportDataFrame extends OpFromJson {
  type SomeAttribute = Attribute[_]

  def toSymbol(field: types.StructField) = Symbol("imported_column_" + field.name)

  // I really don't understand why this isn't part of the spark API, but I can't find it.
  // So here it goes.
  private def typeTagFromDataType(dataType: types.DataType): TypeTag[_] = {
    import scala.reflect.runtime.universe._
    dataType match {
      case at: types.ArrayType => TypeTagUtil.arrayTypeTag(typeTagFromDataType(at.elementType))
      case _: types.BinaryType => typeTag[Array[Byte]]
      case _: types.BooleanType => typeTag[Boolean]
      case _: types.ByteType => typeTag[Byte]
      case _: types.DateType => typeTag[java.sql.Date]
      case _: types.DecimalType => typeTag[java.math.BigDecimal]
      case _: types.DoubleType => typeTag[Double]
      case _: types.FloatType => typeTag[Float]
      case _: types.IntegerType => typeTag[Int]
      case _: types.LongType => typeTag[Long]
      case mt: types.MapType =>
        TypeTagUtil.mapTypeTag(typeTagFromDataType(mt.keyType), typeTagFromDataType(mt.valueType))
      case _: types.ShortType => typeTag[Short]
      case _: types.StringType => typeTag[String]
      case _: types.TimestampType => typeTag[java.sql.Timestamp]
      case st: types.StructType if (
        st.fields.size == 2 && st.fields(0).dataType.isInstanceOf[types.DoubleType]
        && st.fields(1).dataType.isInstanceOf[types.DoubleType]) =>
        typeTag[(Double, Double)]
      case x => throw new AssertionError(s"Unsupported type in DataFrame: $x")
    }
  }

  class Output(schema: types.StructType)(implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    private def attributeFromTypeTag[T: TypeTag](
      ids: => EntityContainer[VertexSet], name: Symbol): EntityContainer[Attribute[T]] =
      vertexAttribute[T](ids, name)

    def attributeFromField(
      ids: => EntityContainer[VertexSet],
      field: types.StructField): EntityContainer[Attribute[_]] = {
      attributeFromTypeTag(ids, toSymbol(field))(typeTagFromDataType(field.dataType))
    }

    val ids = vertexSet
    val columns = schema.map {
      field => field.name -> attributeFromField(ids, field)
    }.toMap
  }
  def fromJson(j: JsValue) = new ImportDataFrame(
    types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType],
    None,
    (j \ "timestamp").as[String])

  def apply(inputFrame: DataFrame) =
    new ImportDataFrame(inputFrame.schema, Some(inputFrame), Timestamp.toString)
}

class ImportDataFrame private (
  val schema: types.StructType,
  inputFrame: Option[DataFrame],
  val timestamp: String)
    extends TypedMetaGraphOp[NoInput, ImportDataFrame.Output] {

  override def equals(other: Any): Boolean =
    other match {
      case otherOp: ImportDataFrame =>
        (otherOp.schema == schema) && (otherOp.timestamp == timestamp)
      case _ => false
    }

  override lazy val hashCode = gUID.hashCode

  import ImportDataFrame._
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output(schema)(instance)
  override def toJson = Json.obj("schema" -> schema.prettyJson, "timestamp" -> timestamp)

  private def toNumberedLines(rc: RuntimeContext): AttributeRDD[Seq[Any]] = {
    val df = inputFrame.get
    val numRows = df.count()
    val maxRows = Limitations.maxImportedLines
    if (maxRows >= 0) {
      if (numRows > maxRows) {
        throw new AssertionError(
          s"Can't import $numRows lines as your licence only allows $maxRows.")
      }
    }
    val partitioner = rc.partitionerForNRows(numRows)
    import com.lynxanalytics.biggraph.spark_util.Implicits._
    df.rdd.randomNumbered(partitioner).mapValues(_.toSeq)
  }

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    assert(
      inputFrame.nonEmpty,
      "You cannot rerun an ImportDataFrame operation. If the result is not in the data directory" +
        " then you have to reimport the data using a new instance of ImportDataFrame")

    val sc = rc.sparkContext
    sc.setLocalProperty("spark.sql.execution.id", null) // No idea what I am doing.

    val entities = o.columns.values.map(_.entity)
    val entitiesByName = entities.map(e => (e.name, e): (Symbol, Attribute[_])).toMap
    val inOrder = schema.map(f => entitiesByName(toSymbol(f)))

    rc.ioContext.writeAttributes(inOrder, toNumberedLines(rc))
  }
}
