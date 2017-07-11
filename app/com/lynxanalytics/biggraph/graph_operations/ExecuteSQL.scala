// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.ProtoTable
import com.lynxanalytics.biggraph.graph_api._
import org.apache.spark
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

object ExecuteSQL extends OpFromJson {
  type Alias = String
  type TableName = String
  case class OptimizedPlanWithLookup(plan: LogicalPlan, lookup: Map[Alias, (TableName, ProtoTable)])
  object OptimizedPlanWithLookup {
    def apply(sqlQuery: String,
              protoTables: Map[TableName, ProtoTable]): OptimizedPlanWithLookup = {
      import spark.sql.catalyst.analysis._
      import spark.sql.catalyst.catalog._
      import spark.sql.catalyst.expressions._
      import spark.sql.catalyst.plans.logical._
      val sqlConf = spark.sql.SQLHelperHelper.newSQLConf
      val parser = new SparkSqlParser(sqlConf)
      val unanalyzedPlan = parser.parsePlan(sqlQuery)
      val aliasTableLookup = new mutable.HashMap[String, (TableName, ProtoTable)]()
      val planResolved = unanalyzedPlan.resolveOperators {
        case u: UnresolvedRelation =>
          assert(protoTables.contains(u.tableName), s"No such table: ${u.tableName}")
          val protoTable = protoTables(u.tableName)
          val attributes = protoTable.schema.map {
            f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
          }
          val rel = LocalRelation(attributes)
          val name = u.alias.getOrElse(u.tableIdentifier.table)
          aliasTableLookup(name) = (u.tableIdentifier.table, protoTable)
          SubqueryAlias(name, rel, Some(u.tableIdentifier))
      }
      val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, sqlConf)
      val analyzer = new Analyzer(catalog, sqlConf)
      val analyzedPlan = analyzer.execute(planResolved)
      val optimizer = new SchemaInferencingOptimizer(catalog, sqlConf)
      new OptimizedPlanWithLookup(optimizer.execute(analyzedPlan), aliasTableLookup.toMap)
    }
  }

  class Input(inputTables: Set[String]) extends MagicInputSignature {
    val tables = inputTables.map(name => table(Symbol(name)))
  }
  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val t = table(schema)
  }

  def fromJson(j: JsValue) = {
    new ExecuteSQL(
      (j \ "sqlQuery").as[String],
      (j \ "inputTables").as[Set[String]],
      types.DataType.fromJson((j \ "outputSchema").as[String])
        .asInstanceOf[types.StructType]
    )
  }

  private def run(sqlQuery: String, outputSchema: StructType,
                  tables: Map[TableName, Table])(implicit m: MetaGraphManager): Table = {
    import Scripting._
    val op = ExecuteSQL(sqlQuery, tables.keySet, outputSchema)
    op.tables.foldLeft(InstanceBuilder(op)) {
      case (builder, template) => builder(template, tables(template.name.name))
    }.result.t
  }

  def run(sqlQuery: String,
          protoTables: Map[String, ProtoTable])(implicit m: MetaGraphManager): Table = {
    val planWithLookup = OptimizedPlanWithLookup(sqlQuery, protoTables)
    val minimizedProtoTables = ProtoTable.minimize(planWithLookup.plan, planWithLookup.lookup)
    val tables = minimizedProtoTables.mapValues(protoTable => protoTable.toTable)
    ExecuteSQL.run(sqlQuery, planWithLookup.plan.schema, tables)
  }

}

import ExecuteSQL._
case class ExecuteSQL(
    sqlQuery: String,
    inputTables: Set[String],
    outputSchema: types.StructType) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = false // Optimize for the case where the user just wants to see a sample.
  @transient override lazy val inputs = new Input(inputTables)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output(outputSchema)(instance)
  override def toJson = Json.obj(
    "sqlQuery" -> sqlQuery,
    "inputTables" -> inputTables,
    "outputSchema" -> outputSchema.prettyJson)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val sqlContext = rc.dataManager.masterSQLContext // TODO: Use a newSQLContext() instead.
    val dfs = inputs.tables.map { t => t.name.name -> t.df }
    val df = DataManager.sql(sqlContext, sqlQuery, dfs.toList)
    output(o.t, df)
  }
}

// We don't use this optimizer to process data (that's taken care of by Spark).
// This is just used to push down the projections to the leaf nodes, to avoid calculating fields in
// the data we don't need.
class SchemaInferencingOptimizer(
  catalog: SessionCatalog,
  conf: SQLConf)
    extends Optimizer(catalog, conf) {
  val weDontWant = Set("Finish Analysis", "LocalRelation")
  override def batches: Seq[Batch] = super.batches
    .filter(b => !weDontWant.contains(b.name))
}
