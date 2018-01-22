// Creating a Table from a bunch of Attributes enforces the computation of those attributes before
// the Table can be used. A ProtoTable allows creating a Table that only depends on a subset of
// attributes.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join

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
  // For Spark SQL plans.
  val relation: LocalRelation = {
    // We use the hashcode of the ProtoTable for comparison. The optimizer modifies other fields,
    // but we can still match each column to the ProtoTable based on each hashcode.
    val fields = schema.fields.map(f => f.withComment(s"$this"))
    if (fields.nonEmpty) {
      LocalRelation(fields.head, fields.tail: _*)
    } else {
      LocalRelation()
    }
  }
}

object ProtoTable {
  def apply(table: Table) = new TableWrappingProtoTable(table)
  def apply(vs: VertexSet, attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager) =
    new AttributesProtoTable(vs, attributes)
  def scalar(scalars: Iterable[(String, Scalar[_])])(implicit m: MetaGraphManager) =
    new ScalarsProtoTable(scalars)

  // Analyzes the given query and restricts the given ProtoTables to their minimal subsets that is
  // necessary to support the query.
  def minimize(
    plan: LogicalPlan,
    protoTables: Map[String, ProtoTable]): Map[String, ProtoTable] = {
    // Match tables to ProtoTables based on the comment added in ProtoTable.relation
    val protoStrings = plan.collectLeaves().flatMap(_.output.map(_.metadata.getString("comment")))
    val fields = getRequiredFields(plan).map(f => (f.name, f.metadata))
    val selectedTables = protoTables.mapValues {
      f =>
        val output = f.relation.output
        val newSchema = fields.intersect(output.map(f => (f.name, f.metadata)))
        if (newSchema.nonEmpty)
          f.maybeSelect(newSchema.map(_._1))
        else if (protoStrings.contains(f.toString))
          f.maybeSelect(Seq(output.map(_.name).head))
        else
          f.maybeSelect(Seq())
    }.filter(_._2.schema.length > 0)
    selectedTables
  }

  private def getRequiredFields(plan: LogicalPlan): Seq[NamedExpression] =
    plan match {
      case p: Project =>
        p.references.toSeq ++ getRequiredFields(p.child)
      case Filter(expression, child) =>
        expression.references.toSeq ++ getRequiredFields(child)
      case Join(left, right, _, condition) =>
        (condition match {
          case Some(exp) => exp.references.toSeq
          case None => Seq()
        }) ++ getRequiredFields(left) ++ getRequiredFields(right)
      case l: LeafNode =>
        bigGraphLogger.info(s"$l ignored in ProtoTable minimalization")
        Seq()
      case s: UnaryNode => getRequiredFields(s.child)
      case s: BinaryNode =>
        getRequiredFields(s.left) ++ getRequiredFields(s.right)
      case s: Union =>
        s.children.flatMap(getRequiredFields)
    }
}

class TableWrappingProtoTable(table: Table) extends ProtoTable {
  def schema = table.schema
  // Tables are atomic metagraph entities, so we use the whole thing even if only parts are needed.
  def maybeSelect(columns: Iterable[String]) = this
  def toTable = table
}

class AttributesProtoTable(
    vs: VertexSet,
    attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager) extends ProtoTable {
  lazy val schema = spark_util.SQLHelper.dataFrameSchema(attributes)
  def maybeSelect(columns: Iterable[String]) = {
    val keep = columns.toSet
    new AttributesProtoTable(vs, attributes.filter { case (name, attr) => keep.contains(name) })
  }
  def toTable = graph_operations.AttributesToTable.run(vs, attributes)
}

class ScalarsProtoTable(
    scalars: Iterable[(String, Scalar[_])])(implicit m: MetaGraphManager) extends ProtoTable {
  lazy val schema = spark_util.SQLHelper.dataFrameSchemaScalar(scalars)

  def maybeSelect(columns: Iterable[String]) = {
    val keep = columns.toSet
    new ScalarsProtoTable(scalars.filter { case (name, attr) => keep.contains(name) })
  }

  def toTable: Table = {
    graph_operations.ScalarsToTable.run(scalars)
  }
}
