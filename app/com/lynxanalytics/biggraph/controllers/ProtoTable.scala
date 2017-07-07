// Creating a Table from a bunch of Attributes enforces the computation of those attributes before
// the Table can be used. A ProtoTable allows creating a Table that only depends on a subset of
// attributes.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types

import scala.collection.mutable

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
  def minimize(optimizedPlan: LogicalPlan,
               protoTables: Map[String, (String, ProtoTable)]): Map[String, ProtoTable] = {
    val tables = getRequiredFields(optimizedPlan)
    val selectedTables = tables.map(f => {
      val (name, table) = protoTables(f._1)
      val columns = f._2.flatMap(parseExpression)
      val selectedTable = if (columns.contains("*")) {
        table
      } else {
        table.maybeSelect(columns)
      }
      name -> selectedTable
    }).toMap
    selectedTables
  }

  private def parseExpression(expression: Expression): Seq[String] = expression match {
    case u: AttributeReference => Seq(u.name)
    case u: UnresolvedStar => Seq("*")
    case exp: Expression => exp.children.flatMap(parseExpression)
  }

  private def getRequiredFields(plan: LogicalPlan): List[(String, Seq[NamedExpression])] =
    plan match {
      case SubqueryAlias(name, Project(projectList, _), _) =>
        List((name, projectList))
      case u: SubqueryAlias =>
        List((u.alias, Seq(UnresolvedStar(target = None))))
      case l: LeafNode =>
        bigGraphLogger.info(s"$l ignored in ProtoTable minimalization")
        List()
      case s: UnaryNode => getRequiredFields(s.child)
      case s: BinaryNode =>
        getRequiredFields(s.left) ++ getRequiredFields(s.right)
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
