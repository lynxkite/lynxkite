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

case class UnresolvedColumnException(message: String, trace: Throwable)
  extends Exception(message, trace)

object ExecuteSQL extends OpFromJson {
  def getLogicalPlan(
    sqlQuery: String,
    protoTables: Map[String, ProtoTable]): LogicalPlan = {
    import spark.sql.catalyst.analysis._
    import spark.sql.catalyst.catalog._
    import spark.sql.catalyst.expressions._
    import spark.sql.catalyst.plans.logical._
    val sqlConf = new spark.sql.internal.SQLConf()
    val parser = new SparkSqlParser(sqlConf)
    val parsedPlan = parser.parsePlan(sqlQuery)
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, sqlConf)
    catalog.createDatabase(
      CatalogDatabase(
        name = "default", description = "", locationUri = new java.net.URI("loc"),
        properties = Map.empty),
      ignoreIfExists = false)
    for ((name, table) <- protoTables) {
      catalog.createTempView(name, table.getRelation, overrideIfExists = true)
    }
    val analyzer = new Analyzer(catalog, sqlConf)
    val analyzedPlan = analyzer.execute(parsedPlan)
    new SchemaInferencingOptimizer(catalog, sqlConf).execute(analyzedPlan)
  }

  class Input(inputTables: Set[String]) extends MagicInputSignature {
    val tables = inputTables.map(name => table(Symbol(name)))
  }
  class Output(schema: types.StructType)(
      implicit
      instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val t = table(schema)
  }

  def fromJson(j: JsValue) = {
    new ExecuteSQL(
      (j \ "sqlQuery").as[String],
      (j \ "inputTables").as[Set[String]],
      types.DataType.fromJson((j \ "outputSchema").as[String])
        .asInstanceOf[types.StructType])
  }

  private def run(sqlQuery: String, outputSchema: StructType,
    tables: Map[String, Table])(implicit m: MetaGraphManager): Table = {
    import Scripting._
    val op = ExecuteSQL(sqlQuery, tables.keySet, outputSchema)
    op.tables.foldLeft(InstanceBuilder(op)) {
      case (builder, template) => builder(template, tables(template.name.name))
    }.result.t
  }

  def run(
    sqlQuery: String,
    protoTables: Map[String, ProtoTable])(implicit m: MetaGraphManager): Table = {
    val plan = getLogicalPlan(sqlQuery, protoTables)
    val minimizedProtoTables = ProtoTable.minimize(plan, protoTables)
    val tables = minimizedProtoTables.mapValues(protoTable => protoTable.toTable)
    ExecuteSQL.run(sqlQuery, plan.schema, tables)
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

  def execute(
    inputDatas: DataSet,
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
// The optimizer minimizes the calculations needed to get the results. The only part that we need
// from this is that projections get pushed down as much as possible, i.e. with the query
// select a from b where c=d, the projection pushdown would make sure that we only request
// a, b, c, and d from our data source (ProtoTables in our case).
class SchemaInferencingOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf)
  extends Optimizer(catalog, conf) {
  val weDontWant = Set("Finish Analysis", "LocalRelation")
  override def batches: Seq[Batch] = super.batches
    .filter(b => !weDontWant.contains(b.name))
}
