// Frontend operations for importing data from outside of the workspace.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.{JDBCUtil}
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.spark_util.SQLHelper

class ImportOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.ImportOperations

  override val defaultIcon = "upload"

  def registerImport(id: String)(factory: Context => ImportOperation): Unit = {
    registerOp(id, defaultIcon, category, List(), List("table"), factory)
  }

  import OperationParams._
  import org.apache.spark

  def simpleFileName(name: String): String = {
    // Drop the hash if this is an upload.
    if (name.startsWith("UPLOAD$")) name.split("/", -1).last.split("\\.", 2).last
    // Include the directory if the filename is a wildcard.
    else if (name.split("/", -1).last.contains("*")) name.split("/", -1).takeRight(2).mkString("/")
    else name.split("/", -1).last
  }

  registerImport("Import CSV")(new ImportOperation(_) {
    params ++= List(
      FileParam("filename", "File"),
      Param(
        "columns",
        "Columns in file",
        placeholder = "Leave empty to read header.",
        group = "Advanced settings"),
      Code("delimiter", "Delimiter", defaultValue = ",", group = "Advanced settings", language = ""),
      Code("quote", "Quote character", defaultValue = "\"", group = "Advanced settings", language = ""),
      Code("escape", "Escape character", defaultValue = "\\", group = "Advanced settings", language = ""),
      Code("null_value", "Null value", defaultValue = "", group = "Advanced settings", language = ""),
      Code("date_format", "Date format", defaultValue = "yyyy-MM-dd", group = "Advanced settings", language = ""),
      Code(
        "timestamp_format",
        "Timestamp format",
        defaultValue = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        group = "Advanced settings",
        language = ""),
      Choice(
        "ignore_leading_white_space",
        "Ignore leading white space",
        options = FEOption.noyes,
        group = "Advanced settings"),
      Choice(
        "ignore_trailing_white_space",
        "Ignore trailing white space",
        options = FEOption.noyes,
        group = "Advanced settings"),
      Code("comment", "Comment character", defaultValue = "", group = "Advanced settings", language = ""),
      Choice(
        "error_handling",
        "Error handling",
        List(
          FEOption("FAILFAST", "Fail on any malformed line"),
          FEOption("DROPMALFORMED", "Ignore malformed lines"),
          FEOption("PERMISSIVE", "Salvage malformed lines: truncate or fill with nulls"),
        ),
        group = "Advanced settings",
      ),
      Choice("infer", "Infer types", options = FEOption.noyes, group = "Advanced settings"),
      Param(
        "imported_columns",
        "Columns to import",
        placeholder = "Leave empty to import all columns.",
        group = "Advanced settings"),
      Param("limit", "Limit", group = "Advanced settings"),
      Code("sql", "SQL", language = "sql", group = "Advanced settings"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )

    override def summary = {
      if (params("filename").nonEmpty) {
        val fn = simpleFileName(params("filename"))
        s"Import $fn"
      } else "Import CSV"
    }

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      val errorHandling = params("error_handling")
      val infer = params("infer") == "yes"
      val ignoreLeadingWhiteSpace = params("ignore_leading_white_space") == "yes"
      val ignoreTrailingWhiteSpace = params("ignore_trailing_white_space") == "yes"
      val columns = params("columns")
      assert(
        Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST").contains(errorHandling),
        s"Unrecognized error handling mode: $errorHandling")
      assert(!infer || columns.isEmpty, "List of columns cannot be set when using type inference.")
      val reader = context
        .read
        .format("csv")
        .option("mode", errorHandling)
        .option("sep", params("delimiter"))
        .option("quote", params("quote"))
        .option("escape", params("escape"))
        .option("nullValue", params("null_value"))
        .option("dateFormat", params("date_format"))
        .option("timestampFormat", params("timestamp_format"))
        .option("ignoreLeadingWhiteSpace", if (ignoreLeadingWhiteSpace) "true" else "false")
        .option("ignoreTrailingWhiteSpace", if (ignoreTrailingWhiteSpace) "true" else "false")
        .option("comment", params("comment"))
        .option("inferSchema", if (infer) "true" else "false")
      val readerWithSchema =
        if (columns.nonEmpty) {
          reader.schema(SQLController.stringOnlySchema(columns.split(",", -1)))
        } else {
          // Read column names from header.
          reader.option("header", "true")
        }
      val hadoopFile = graph_util.HadoopFile(params("filename"))
      hadoopFile.assertReadAllowedFrom(user)
      FileImportValidator.checkFileHasContents(hadoopFile)
      readerWithSchema.load(hadoopFile.resolvedName)
    }
  })

  registerImport("Import JDBC")(new ImportOperation(_) {
    params ++= List(
      Param("jdbc_url", "JDBC URL"),
      Param("jdbc_table", "JDBC table"),
      Param("key_column", "Key column"),
      NonNegInt("num_partitions", "Number of partitions", default = 0),
      Param("partition_predicates", "Partition predicates"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Param("connection_properties", "Connection properties"),
      Code("sql", "SQL", language = "sql"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      JDBCUtil.read(
        context,
        params("jdbc_url"),
        params("jdbc_table"),
        params("key_column"),
        params("num_partitions").toInt,
        splitParam("partition_predicates").toList,
        splitParam("connection_properties").map { e =>
          val eq = e.indexOf("=")
          assert(
            eq != -1,
            s"Invalid connection property definition: ${params("connection_properties")}")
          e.take(eq) -> e.drop(eq + 1)
        }.toMap,
      )
    }
  })

  register("Import from Neo4j", List(), List("graph"))(new SimpleOperation(_) with Importer {
    import play.api.libs.json
    import com.lynxanalytics.biggraph.controllers._

    params ++= List(
      Param("url", "Neo4j connection", defaultValue = "bolt://localhost:7687"),
      Param("username", "Neo4j username", defaultValue = "neo4j"),
      Param("password", "Neo4j password", defaultValue = "neo4j"),
      Code("vertex_query", "Vertex query", defaultValue = "MATCH (node) RETURN node", language = ""),
      Code("edge_query", "Edge query", defaultValue = "MATCH ()-[rel]->() RETURN rel", language = ""),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )

    override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
      params.validate()
      assert(params("imported_table").nonEmpty, "You have to import the data first.")
      assert(!areSettingsStale, areSettingsStaleReplyMessage)
      import CommonProjectState._
      val state = json.Json.parse(params("imported_table")).as[CommonProjectState]
      Map(context.box.output(context.meta.outputs(0)) -> BoxOutputState.from(state))
    }

    def isNameValid(name: String) = {
      try {
        SubProject.validateName(name)
        true
      } catch {
        case e: AssertionError => false
      }
    }

    override def runImport(env: com.lynxanalytics.biggraph.BigGraphEnvironment): ImportResult = {
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types
      import com.lynxanalytics.biggraph.graph_util.Scripting._
      val spark = SQLController.defaultContext()(env.sparkDomain)
      val project = new RootProjectEditor(CommonProjectState.emptyState)
      def runQuery(query: String, column: String) = {
        import spark.implicits._
        val json = spark.read.format("org.neo4j.spark.DataSource")
          .option("authentication.type", "basic")
          .option("authentication.basic.username", params("username"))
          .option("authentication.basic.password", params("password"))
          .option("url", params("url"))
          .option("schema.strategy", "string")
          .option("query", query)
          .load
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        assert(json.columns.contains(column), s"You must set a query that returns '$column'.")
        spark.read.json(json.select(col(column)).as[String])
      }
      // Get vertices.
      if (params("vertex_query").nonEmpty) {
        val df = runQuery(params("vertex_query"), "node")
        val table = graph_operations.ImportDataFrame.run(df)
        val vs = table.toProject
        project.vertexSet = vs.vertexSet
        project.vertexAttributes = vs.viewer.vertexAttributes
      }
      // Get edges.
      if (params("edge_query").nonEmpty) {
        val df = runQuery(params("edge_query"), "rel")
        val table = graph_operations.ImportDataFrame.run(df)
        val esp = table.toProject.viewer
        val srcAttr = esp.vertexAttributes("<source.id>").asString
        val dstAttr = esp.vertexAttributes("<target.id>").asString
        def pullAttributes(embedding: EdgeBundle) = {
          for ((name, attr) <- esp.vertexAttributes) {
            // LynxKite attribute names cannot have '.'.
            val renamed = name.replace(".", "_")
            // Skip names that have other issues.
            if (isNameValid(renamed)) {
              project.edgeAttributes(renamed) = attr.pullVia(embedding)
            }
          }
        }
        if (params("vertex_query").isEmpty) {
          val es = {
            val op = graph_operations.VerticesToEdges()
            op(op.srcAttr, srcAttr)(op.dstAttr, dstAttr).result
          }
          project.vertexSet = es.vs
          project.newVertexAttribute("<id>", es.stringId)
          project.edgeBundle = es.es
          pullAttributes(es.embedding)
        } else {
          val idAttr = project.vertexAttributes("<id>").asString
          val es = graph_operations.ImportEdgesForExistingVertices.run(
            idAttr,
            idAttr,
            srcAttr,
            dstAttr)
          project.edgeBundle = es.edges
          pullAttributes(es.embedding)
        }
      }
      val state = json.Json.toJson(project.state).toString
      ImportResult(state, this.settingsString())
    }
  })

  registerImport("Import from Neo4j files")(new ImportOperation(_) {
    params += Param("path", "Path to Neostore")
    params += Choice("what", "What to import", options = FEOption.list("nodes", "relationships"))
    params("what") match {
      // In both cases the parameter ID is "filter", so
      // it's not becoming unrecognized when you switch.
      case "nodes" => params += Param("filter", "Labels to import")
      case "relationships" => params += Param("filter", "Relationship types to import")
    }
    params += Param("properties", "Properties to import")
    params += ImportedDataParam()
    params += new DummyParam("last_settings", "")

    override def summary = params("what") match {
      case "nodes" => "Import nodes from Neo4j files"
      case "relationships" => "Import relationships from Neo4j files"
    }
    override def getDataFrame(context: spark.sql.SQLContext) = {
      params("what") match {
        case "nodes" => Neo4j.readNodes(
            context.sparkSession,
            params("path"),
            splitParam("filter"),
            properties)
        case "relationships" => Neo4j.readRelationships(
            context.sparkSession,
            params("path"),
            splitParam("filter"),
            properties)
      }
    }
    def getRawDataFrame(context: spark.sql.SQLContext) = ??? // Not used.

    def properties: Seq[(String, SerializableType[_])] = {
      val re = raw"\s*(\S+)\s*:\s*(\S+)\s*".r
      splitParam("properties").map {
        case re(name, tpe) => (name, SerializableType.TypeParser.parse(tpe))
        case x => throw new AssertionError(s"Properties must be listed as 'name: type'. Got '$x'.")
      }
    }
  })

  abstract class FileWithSchemaBase(context: Context) extends ImportOperation(context) {
    val format: String

    override def summary = {
      val fn = simpleFileName(params("filename"))
      s"Import $fn"
    }

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      val hadoopFile = graph_util.HadoopFile(params("filename"))
      hadoopFile.assertReadAllowedFrom(user)
      FileImportValidator.checkFileHasContents(hadoopFile)
      context.read.format(format).load(hadoopFile.resolvedName)
    }
  }

  // Also adds parameters.
  abstract class FileWithSchema(context: Context) extends FileWithSchemaBase(context) {
    params ++= List(
      FileParam("filename", "File"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )
  }

  registerImport("Import ORC")(new FileWithSchema(_) { val format = "orc" })
  registerImport("Import JSON")(new FileWithSchema(_) { val format = "json" })
  registerImport("Import AVRO")(new FileWithSchema(_) { val format = "avro" })

  registerImport("Import Delta")(new FileWithSchema(_) {
    val format = "delta"
    params ++= List(
      Param("version_as_of", "versionAsOf"))

    override def getRawDataFrame(context: spark.sql.SQLContext) = {
      val hadoopFile = graph_util.HadoopFile(params("filename"))
      hadoopFile.assertReadAllowedFrom(user)
      FileImportValidator.checkFileHasContents(hadoopFile)

      if (params("version_as_of").isEmpty) {
        context.read.format(format).load(hadoopFile.resolvedName)
      } else {
        context.read.format(format).option("versionAsOf", params("version_as_of")).load(hadoopFile.resolvedName)
      }
    }
  })

  registerImport("Import Parquet")(new FileWithSchemaBase(_) {
    val format = "parquet"
    params ++= List(
      FileParam("filename", "File"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      Choice(
        "eager",
        "Import now or provide schema",
        List(FEOption("yes", "Import now"), FEOption("no", "Provide schema"))),
      new DummyParam("last_settings", ""),
    )
    params ++= (params("eager") match { // Hide/show import button and schema parameter.
      case "yes" => List(
          ImportedDataParam(),
          new DummyParam("schema", ""),
        )
      case "no" => List(
          new DummyParam("imported_table", ""),
          Param("schema", "Schema"),
        )
    })

    override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
      params.validate()
      params("eager") match {
        case "yes" =>
          assert(params("imported_table").nonEmpty, "You have to import the data first.")
          assert(!areSettingsStale, areSettingsStaleReplyMessage)
          makeOutput(tableFromGuid(params("imported_table")))
        case "no" =>
          makeOutput(graph_operations.ReadParquetWithSchema.run(
            params("filename"),
            splitParam("schema", delimiter = ";")))
      }
    }
  })

  registerImport("Import from BigQuery (raw table)")(new ImportOperation(_) {
    params ++= List(
      Param("parent_project_id", "GCP project ID for billing"),
      Param("project_id", "GCP project ID for importing"),
      Param("dataset_id", "BigQuery dataset ID for importing"),
      Param("table_id", "BigQuery table ID to import"),
      Param("credentials_file", "Path to Service Account JSON key"),
      Choice("views_enabled", "Allow reading from BigQuery views", options = FEOption.noyes),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      import com.google.cloud.spark.bigquery._
      var df = context.read.format("bigquery")
      if (params("credentials_file").nonEmpty) df = df.option("credentialsFile", params("credentials_file"))
      if (params("parent_project_id").nonEmpty) df = df.option("parentProject", params("parent_project_id"))
      if (params("project_id").nonEmpty) df = df.option("project", params("project_id"))
      if (params("dataset_id").nonEmpty) df = df.option("dataset", params("dataset_id"))
      if (params("views_enabled") == "yes") df = df.option("viewsEnabled", "true")
      df.load(params("table_id"))
    }
  })

  registerImport("Import from BigQuery (Standard SQL result)")(new ImportOperation(_) {
    params ++= List(
      Param("parent_project_id", "GCP project ID for billing"),
      Param("materialization_project_id", "GCP project for materialization"),
      Param("materialization_dataset_id", "BigQuery Dataset for materialization"),
      Code("bq_standard_sql", "BigQuery Standard SQL", language = "sql"),
      Param("credentials_file", "Path to Service Account JSON key"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL (in LynxKite)", language = "sql"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      import com.google.cloud.spark.bigquery._
      var df = context.read.format("bigquery")
        .option("viewsEnabled", "true")
        .option("materializationDataset", params("materialization_dataset_id"))
      if (params("credentials_file").nonEmpty) df = df.option("credentialsFile", params("credentials_file"))
      if (params("materialization_project_id").nonEmpty)
        df = df.option("materializationProject", params("materialization_project_id"))
      if (params("parent_project_id").nonEmpty) df = df.option("parentProject", params("parent_project_id"))
      df.load(params("bq_standard_sql"))
    }
  })

  registerImport("Import from Hive")(new ImportOperation(_) {
    params ++= List(
      Param("hive_table", "Hive table"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedDataParam(),
      new DummyParam("last_settings", ""),
    )
    def getRawDataFrame(context: spark.sql.SQLContext) = {
      assert(
        SparkDomain.hiveConfigured,
        "Hive is not configured for this LynxKite instance. Contact your system administrator.")
      context.table(params("hive_table"))
    }
  })

  register("Import snapshot", List(), List("state"))(new SimpleOperation(_) {
    params += Param("path", "Path")
    override def getOutputs() = {
      val snapshot = DirectoryEntry.fromName(params("path")).asSnapshotFrame
      Map(context.box.output("state") -> snapshot.getState)
    }
  })

  register("Import union of table snapshots", List(), List("table"))(new SimpleOperation(_) {
    params += Param("paths", "Paths")
    override def getOutputs() = {
      val paths = params("paths").split(",")
      val tables = paths.map { path => DirectoryEntry.fromName(path).asSnapshotFrame.getState().table }
      val schema = tables.head.schema
      for (table <- tables) {
        SQLHelper.assertTableHasCorrectSchema(table, schema)
      }
      val protoTables = tables.zipWithIndex.map { case (table, i) => i.toString -> ProtoTable(table) }.toMap
      val sql = (0 until protoTables.size).map { i => s"select * from `$i`" }.mkString(" union all ")
      val result = graph_operations.ExecuteSQL.run(sql, protoTables)
      Map(context.box.output("table") -> BoxOutputState.from(result))
    }
  })

  register("Import well-known graph dataset", List(), List("graph"))(
    new ProjectOutputOperation(_) {
      params += Choice(
        "name",
        "Name",
        options = FEOption.list(
          "Cora",
          "CiteSeer",
          "Karate Club",
          "PubMed"))
      override def summary = {
        val fn = params("name")
        s"Import $fn"
      }
      def enabled = FEStatus.enabled
      def apply() = {
        val g = graph_operations.PyTorchGeometricDataset(params("name"))().result
        project.vertexSet = g.vs
        project.edgeBundle = g.es
        project.newVertexAttribute("x", g.x)
        project.newVertexAttribute("y", g.y)
      }
    })
}
