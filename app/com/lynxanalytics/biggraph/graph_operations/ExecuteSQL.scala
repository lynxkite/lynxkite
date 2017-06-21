// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark
import org.apache.spark.sql.types

object ExecuteSQL extends OpFromJson {
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

  def getLogicalPlan(
    sqlQuery: String, tables: Map[String, Table]): spark.sql.catalyst.plans.logical.LogicalPlan = {
    import spark.sql.SQLHelperHelper
    import spark.sql.catalyst._
    import spark.sql.catalyst.analysis._
    import spark.sql.catalyst.catalog._
    import spark.sql.catalyst.expressions._
    import spark.sql.catalyst.plans.logical._
    val parser = new spark.sql.execution.SparkSqlParser(
      spark.sql.SQLHelperHelper.newSQLConf)
    // Parse the query.
    val planParsed = parser.parsePlan(sqlQuery)
    // Resolve the table references with our tables.
    val planResolved = planParsed.resolveOperators {
      case u: UnresolvedRelation =>
        assert(tables.contains(u.tableName), s"No such table: ${u.tableName}")
        val attributes = tables(u.tableName).schema.map {
          f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        }
        val rel = LocalRelation(attributes)
        val name = u.alias.getOrElse(u.tableIdentifier.table)
        SubqueryAlias(name, rel, None)
    }
    // Do the rest of the analysis.
    val conf = new SimpleCatalystConf(caseSensitiveAnalysis = false)
    val analyzer = new Analyzer(
      new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf), conf)
    analyzer.execute(planResolved)
  }

  def run(sqlQuery: String, tables: Map[String, Table])(implicit m: MetaGraphManager): Table = {
    import Scripting._
    val outputSchema = getLogicalPlan(sqlQuery, tables).schema
    val op = ExecuteSQL(sqlQuery, tables.keySet, outputSchema)
    op.tables.foldLeft(InstanceBuilder(op)) {
      case (builder, template) => builder(template, tables(template.name.name))
    }.result.t
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
