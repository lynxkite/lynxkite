// Creating a Table from a bunch of Attributes enforces the computation of those attributes before
// the Table can be used. A ProtoTable allows creating a Table that only depends on a subset of
// attributes.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.execution.OptimizeMetadataOnlyQuery
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.python.ExtractPythonUDFFromAggregate
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

class SparkOptimizer(
                      catalog: SessionCatalog,
                      conf: SQLConf)
  extends Optimizer(catalog, conf) {

  override def batches: Seq[Batch] = super.batches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog, conf)) :+
    Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate)
//    Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions)
}

object ProtoTable {
  def apply(table: Table) = new TableWrappingProtoTable(table)
  def apply(attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager) =
    new AttributesProtoTable(attributes)

  // Analyzes the given query and restricts the given ProtoTables to their minimal subsets that is
  // necessary to support the query.
  def minimize(sql: String, protoTables: Map[String, ProtoTable]): Map[String, ProtoTable] = {
    val sqlConf = new SQLConf()
    val parser = new SparkSqlParser(sqlConf)
    val unanalyzedPlan = parser.parsePlan(sql.replace("|","__"))
    unanalyzedPlan.allAttributes
    val catalog = new SessionCatalog(new InMemoryCatalog())
    val db = CatalogDatabase("default", "", "ot", Map())
    catalog.createDatabase(db, ignoreIfExists = false)
    for ((name, protoTable) <- protoTables)
      catalog.createTable(CatalogTable(
      TableIdentifier(name.replace("|","__")),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty,
      protoTable.schema), ignoreIfExists = false)
    val anal = new Analyzer(catalog, new CatalystConf())
    val analyzedPlan = anal.execute(unanalyzedPlan)
    val optimizer = new SparkOptimizer(catalog, sqlConf)
    val optimizedPlan = optimizer.execute(analyzedPlan)
    val tables = parsePlanForRequiredTables(optimizedPlan)
    val selectedTables = tables.map( f => {
      val table = protoTables(f._1.replace("__", "|"))
      val columns = f._2.flatMap(parseExpression)
      val selectedTable = if (columns.contains("*")) {
        table
      } else {
        table.maybeSelect(columns)
      }
      f._1.replace("__", "|") -> selectedTable
    }).toMap
    selectedTables
  }

  def parseExpression(expression: Expression): Seq[String] = expression match {
    case u: UnresolvedAttribute => Seq(u.name)
    case u: UnresolvedStar => Seq("*")
    case exp: Expression => exp.children.flatMap(parseExpression)
  }

  def parsePlanForRequiredTables(plan: LogicalPlan): List[(String, Seq[NamedExpression])] =
    plan match {
      case Project(projectList, u: UnresolvedRelation) =>
        List((u.tableIdentifier.identifier, projectList))
      case Project(projectList, Filter(_, u: UnresolvedRelation)) =>
        List((u.tableIdentifier.identifier, projectList))
      case l: LeafNode =>
        bigGraphLogger.info(s"$l ignored in ProtoTable minimalization")
        List()
      case s: UnaryNode => parsePlanForRequiredTables(s.child)
      case s: BinaryNode => parsePlanForRequiredTables(s.left) ++ parsePlanForRequiredTables(s.right)
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
