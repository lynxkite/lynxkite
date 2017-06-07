// Creating a Table from a bunch of Attributes enforces the computation of those attributes before
// the Table can be used. A ProtoTable allows creating a Table that only depends on a subset of
// attributes.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.sql.types

// A kind of wrapper for Tables that can be used for trimming unused dependencies from table
// operations like ExecuteSQL.
trait ProtoTable {
  // The schema of the Table that would be created by toTable.
  protected def schema: types.StructType
  // Returns a ProtoTable that is a possibly smaller subset of this one, still containing the
  // specified columns.
  protected def maybeSelect(columns: Iterable[String]): ProtoTable
  // Creates the promised table.
  def toTable: Table
}

object ProtoTable {
  def apply(table: Table) = new TableWrappingProtoTable(table)
  def apply(attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager) =
    new AttributesProtoTable(attributes)

  // Analyzes the given query and restricts the given ProtoTables to their minimal subsets that is
  // necessary to support the query.
  def minimize(sql: String, protoTables: Map[String, ProtoTable]): Map[String, ProtoTable] = {
    // TODO: Minimize the ProtoTables using SQLHelper.getInputColumns or SparkSqlParser.
    protoTables
  }
}

class TableWrappingProtoTable(table: Table) extends ProtoTable {
  def schema = table.schema
  // Tables are atomic metagraph entities, so we use the whole thing even if only parts are needed.
  def maybeSelect(columns: Iterable[String]) = this
  def toTable = table
}

class AttributesProtoTable(
    attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager) extends ProtoTable {
  lazy val schema = spark_util.SQLHelper.dataFrameSchema(attributes)
  def maybeSelect(columns: Iterable[String]) = {
    val keep = columns.toSet
    new AttributesProtoTable(attributes.filter { case (name, attr) => keep.contains(name) })
  }
  def toTable = graph_operations.AttributesToTable.run(attributes)
}
