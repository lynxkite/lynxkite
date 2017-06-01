package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_api._

import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.protection.Limitations

import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types

import scala.collection.mutable
import scala.reflect.runtime.universe._

import java.util.UUID

object SQLHelper {
  private def toSymbol(field: types.StructField) = Symbol("imported_column_" + field.name)

  private def isTuple2Type(st: types.StructType) =
    st.size == 2 && st(0).name == "_1" && st(1).name == "_2"

  // This should be part of Spark, but is not. SPARK-12264
  def typeTagFromDataType(dataType: types.DataType): TypeTag[_] = {
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
      case _: types.TimestampType => typeTag[java.util.Date]
      case st: types.StructType if isTuple2Type(st) =>
        TypeTagUtil.tuple2TypeTag(
          typeTagFromDataType(st(0).dataType),
          typeTagFromDataType(st(1).dataType))
      case x => throw new AssertionError(s"Unsupported type in DataFrame: $x")
    }
  }

  def dataFrameSchema(columns: Iterable[(String, Attribute[_])]): types.StructType = {
    val fields = columns.map {
      case (name, attr) =>
        types.StructField(
          name = name,
          dataType = typeTagToDataType(attr.typeTag))
    }
    types.StructType(fields.toSeq)
  }

  private def supportedDataType[T: TypeTag]: Option[types.DataType] = {
    try {
      Some(spark.sql.catalyst.ScalaReflection.schemaFor(typeTag[T]).dataType)
    } catch {
      case _: UnsupportedOperationException => None
    }
  }

  def typeTagToDataType[T: TypeTag]: types.DataType = {
    // Convert unsupported types to string.
    supportedDataType[T].getOrElse(types.StringType)
  }

  private def processDataFrameRow(tupleColumnIdList: Seq[Int])(row: Row): Seq[Any] = {
    var result = row.toSeq
    // A simple row.toSeq would be enough for this method, except
    // that tuple-typed columns need special handling.
    for (columnId <- tupleColumnIdList) {
      val column = result(columnId).asInstanceOf[Row]
      result = result.updated(columnId, (column(0), column(1)))
    }
    result
  }

  // Collects positions of columns which contain tuples.
  private def getTupleColumnIdList(schema: types.StructType): Seq[Int] = {
    schema
      .map(field => field.dataType)
      .zipWithIndex
      // Only keep the index of tuple items:
      .collect {
        case (st: types.StructType, id) if isTuple2Type(st) =>
          id
      }
  }

  def toSeqRDD(dataFrame: DataFrame): rdd.RDD[Seq[Any]] = {
    val tupleColumnIdList = getTupleColumnIdList(dataFrame.schema)
    dataFrame.rdd.map(processDataFrameRow(tupleColumnIdList))
  }

  // Make every column nullable. Nullability is not stored in Parquet.
  def allNullable(schema: types.StructType): types.StructType =
    types.StructType(schema.map(_.copy(nullable = true)))
}
