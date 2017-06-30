// Creating a Table from a bunch of Attributes enforces the computation of those attributes before
// the Table can be used. A ProtoTable allows creating a Table that only depends on a subset of
// attributes.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types

// A kind of wrapper for Tables that can be used for trimming unused dependencies from table
// operations like ExecuteSQL.
trait ProtoTable {
  // The schema of the Table that would be created by toTable.
  def schema: types.StructType
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
    val parser = new SparkSqlParser(new SQLConf())
    val tables = parsePlanForTableNames(parser.parsePlan(sql))
    protoTables.filter(p => tables.contains(p._1))
  }

  def parsePlanForTableNames(plan: LogicalPlan): List[String] = {
    plan match {
      case u: UnresolvedRelation => List(u.tableIdentifier.identifier)
      case l: LeafNode =>
        bigGraphLogger.info(s"$l ignored in ProtoTable minimalization")
        List()
      case s: UnaryNode => parsePlanForTableNames(s.child)
      case s: BinaryNode => parsePlanForTableNames(s.left) ++ parsePlanForTableNames(s.right)
    }
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
