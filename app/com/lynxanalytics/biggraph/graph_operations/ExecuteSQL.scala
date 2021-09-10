// Operation for importing data from a DataFrame.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.ProtoTable
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
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
  private lazy val sqlConf = new spark.sql.internal.SQLConf()
  private lazy val parser = new SparkSqlParser
  private lazy val catalog = {
    import spark.sql.catalyst.analysis._
    import spark.sql.catalyst.catalog._
    import spark.sql.catalyst.expressions._
    import spark.sql.catalyst.plans.logical._
    import spark.sql._
    val functionRegistry = FunctionRegistry.builtin
    val reg = UDFHelper.udfRegistration(functionRegistry)
    UDF.register(reg)
    val catalog = new SessionCatalog(new InMemoryCatalog, functionRegistry, sqlConf)
    val locationPath = "file:" + System.getProperty("java.io.tmpdir") + "/lynxkite-executesql-db"
    catalog.createDatabase(
      CatalogDatabase(
        name = "default",
        description = "",
        locationUri = new java.net.URI(locationPath),
        properties = Map.empty),
      ignoreIfExists = false)
    catalog
  }

  def getLogicalPlan(
      sqlQuery: String,
      protoTables: Map[String, ProtoTable]): LogicalPlan = {
    import spark.sql.catalyst.analysis._
    val analyzer = new Analyzer(catalog)
    val parsedPlan = parser.parsePlan(sqlQuery)
    synchronized {
      for ((name, table) <- protoTables) {
        catalog.createTempView(name, table.relation, overrideIfExists = true)
      }
      try {
        analyzer.execute(parsedPlan)
      } finally {
        for ((name, _) <- protoTables) {
          catalog.dropTempView(name)
        }
      }
    }
  }

  class Input(inputTables: List[String]) extends MagicInputSignature {
    val tables = inputTables.map(name => table(Symbol(name)))
  }
  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val t = table(schema)
  }

  def fromJson(j: JsValue) = {
    new ExecuteSQL(
      (j \ "sqlQuery").as[String],
      (j \ "inputTables").as[List[String]],
      types.DataType.fromJson((j \ "outputSchema").as[String])
        .asInstanceOf[types.StructType])
  }

  private def run(sqlQuery: String, outputSchema: StructType, tables: Map[String, Table])(implicit
      m: MetaGraphManager): Table = {
    import Scripting._
    val op = ExecuteSQL(sqlQuery, tables.keySet.toList, outputSchema)
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
    try {
      ExecuteSQL.run(sqlQuery, SQLHelper.stripComment(plan.schema), tables)
    } catch {
      // The exception is thrown on the plan.schema call
      case a: UnresolvedException[_] =>
        throw UnresolvedColumnException(s"${a.treeString} column cannot be found", a)
    }
  }

}

import ExecuteSQL._
case class ExecuteSQL(
    sqlQuery: String,
    inputTables: List[String],
    outputSchema: types.StructType)
    extends SparkOperation[Input, Output] {
  override val isHeavy = false
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
    val sqlContext = rc.sparkDomain.masterSQLContext // TODO: Use a newSQLContext() instead.
    val dfs = inputs.tables.map { t => t.name.name -> t.df }
    val df = SparkDomain.sql(sqlContext, sqlQuery, dfs)
    output(o.t, df)
  }
}
