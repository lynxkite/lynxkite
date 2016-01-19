// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.table
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import org.apache.spark.sql

case class SQLRequest(project: String, sql: String, rownum: Int)
case class SQLResult(header: List[String], data: List[List[String]])

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
    // TODO: this will break for s3 paths if the id or password contains a /. Would be
    // nicer to create an RDD[String] ourselves using HadoopFile.loadTextFile and stuff that
    // into the CSV library. It seems possible via creating a CSVRelation ourselves and turn
    // that into a DataFrame. But both steps seem to require non-public API access. :(
    // So leaving this as is for a first version.
    SQLController.importFromDF(readerWithSchema.load(hadoopFile.resolvedName))
  }

  def runSQLQuery(user: serving.User, request: SQLRequest): SQLResult = {
    val tables = metaManager.synchronized {
      val p = SubProject.parsePath(request.project)
      assert(p.frame.exists, s"Project ${request.project} does not exist.")
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

    log.info(s"Trying to execute query: ${request.sql}")
    val result = sqlContext.sql(request.sql)

    SQLResult(
      header = result.columns.toList,
      data = result.head(request.rownum).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }
}
object SQLController {
  def stringOnlySchema(columns: Seq[String]) = {
    import org.apache.spark.sql.types._
    StructType(columns.map(StructField(_, StringType, true)))
  }

  def importFromDF(df: sql.DataFrame)(
    implicit metaManager: MetaGraphManager, dataManager: DataManager) =
    TableImportResponse(table.TableImport.importDataFrame(df).saveAsCheckpoint)
}
