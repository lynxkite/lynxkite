// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.TableStats
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.table
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DataFrameSpec(project: String, sql: String)
case class SQLQueryRequest(df: DataFrameSpec, maxRows: Int)
case class SQLQueryResult(header: List[String], data: List[List[String]])

case class SQLExportRequest(
  df: DataFrameSpec,
  format: String,
  path: String,
  options: Map[String, String])
case class SQLExportResult(download: Option[String])

case class CSVImportRequest(
    files: String,
    // Name of columns. Empty list means to take column names from the first line of the file.
    columnNames: List[String],
    delimiter: String,
    // One of: PERMISSIVE, DROPMALFORMED or FAILFAST
    mode: String) {
  assert(CSVImportRequest.ValidModes.contains(mode), s"Unrecognized CSV mode: $mode")
}
object CSVImportRequest {
  val ValidModes = Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST")
}

case class JDBCImportRequest(
  jdbcUrl: String,
  table: String,
  keyColumn: String,
  // Empty list means all columns.
  columnsToImport: List[String])

case class TableImportResponse(checkpoint: String)

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager

  def importCSV(user: serving.User, request: CSVImportRequest): TableImportResponse = {
    val reader = dataManager.masterSQLContext
      .read
      .format("com.databricks.spark.csv")
      .option("mode", request.mode)
      .option("delimiter", request.delimiter)
      // We don't want to skip lines starting with #
      .option("comment", null)
    val readerWithSchema = if (request.columnNames.nonEmpty) {
      reader.schema(SQLController.stringOnlySchema(request.columnNames))
    } else {
      // Read column names from header.
      reader.option("header", "true")
    }
    val hadoopFile = HadoopFile(request.files)
    // TODO: #2889
    SQLController.importFromDF(
      readerWithSchema.load(hadoopFile.resolvedName),
      s"Imported from CSV files ${request.files}.")
  }

  def importJDBC(user: serving.User, request: JDBCImportRequest): TableImportResponse = {
    val stats = {
      val connection = java.sql.DriverManager.getConnection(request.jdbcUrl)
      try TableStats(request.table, request.keyColumn)(connection)
      finally connection.close()
    }
    val numPartitions = dataManager.runtimeContext.partitionerForNRows(stats.count).numPartitions
    val fullTable = dataManager.masterSQLContext
      .read
      .jdbc(
        request.jdbcUrl,
        request.table,
        request.keyColumn,
        stats.minKey,
        stats.maxKey,
        numPartitions,
        new java.util.Properties)
    val df = if (request.columnsToImport.nonEmpty) {
      val columns = request.columnsToImport.map(spark.sql.functions.column(_))
      fullTable.select(columns: _*)
    } else fullTable
    val urlSafePart = request.jdbcUrl.takeWhile(_ != '?')
    SQLController.importFromDF(df, s"Imported from table ${request.table} at ${urlSafePart}.")
  }

  private def dfFromSpec(user: serving.User, spec: DataFrameSpec): spark.sql.DataFrame = {
    val tables = metaManager.synchronized {
      val p = SubProject.parsePath(spec.project)
      assert(p.frame.exists, s"Project ${spec.project} does not exist.")
      p.frame.assertReadAllowedFrom(user)

      val v = p.viewer
      v.allRelativeTablePaths.map {
        tableName => (tableName -> Table.fromCanonicalPath(tableName, v))
      }
    }
    // Every query runs in its own SQLContext for isolation.
    val sqlContext = dataManager.newSQLContext()
    for ((tableName, table) <- tables) {
      table.toDF(sqlContext).registerTempTable(tableName)
    }

    log.info(s"Trying to execute query: ${spec.sql}")
    sqlContext.sql(spec.sql)
  }

  def runSQLQuery(user: serving.User, request: SQLQueryRequest): SQLQueryResult = {
    val df = dfFromSpec(user, request.df)

    SQLQueryResult(
      header = df.columns.toList,
      data = df.head(request.maxRows).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }

  def exportSQLQuery(user: serving.User, request: SQLExportRequest): SQLExportResult = {
    val df = dfFromSpec(user, request.df)
    val path = if (request.path == "<download>") {
      dataManager.repositoryPath / "exports" / Timestamp.toString + "." + request.format
    } else {
      HadoopFile(request.path)
    }
    val format = request.format match {
      case "csv" => "com.databricks.spark.csv"
      case x => x
    }
    // TODO: #2889
    df.write.format(format).options(request.options).save(path.resolvedName)
    SQLExportResult(
      download = if (request.path == "<download>") Some(path.symbolicName) else None)
  }
}
object SQLController {
  def stringOnlySchema(columns: Seq[String]) = {
    import spark.sql.types._
    StructType(columns.map(StructField(_, StringType, true)))
  }

  def importFromDF(df: spark.sql.DataFrame, notes: String)(
    implicit metaManager: MetaGraphManager, dataManager: DataManager) =
    TableImportResponse(table.TableImport.importDataFrame(df).saveAsCheckpoint(notes))
}
