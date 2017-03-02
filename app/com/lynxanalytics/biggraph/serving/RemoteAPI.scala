// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import scala.concurrent.Future
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode, types }
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._
import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving.FrontendJson._
import com.lynxanalytics.biggraph.table.TableImport
import org.apache.spark.sql.types.StructType

object RemoteAPIProtocol {
  case class ParquetMetadataResponse(rowCount: Long)
  case class CheckpointResponse(checkpoint: String)
  case class OperationNamesResponse(names: List[String])
  case class BoxMetadatasResponse(boxes: List[BoxMetadata])
  case class OperationRequest(
    checkpoint: String,
    inputs: Map[String, BoxOutput],
    operation: String,
    parameters: Map[String, String])
  case class LoadNameRequest(name: String)
  case class RemoveNameRequest(name: String)
  case class SaveCheckpointRequest(
    checkpoint: String,
    name: String,
    // Defaults to read access only for the creating user.
    readACL: Option[String],
    // Defaults to write access only for the creating user.
    writeACL: Option[String])
  case class ScalarRequest(checkpoint: String, path: List[String], scalar: String)
  case class HistogramRequest(
    checkpoint: String,
    path: List[String],
    attr: String,
    numBuckets: Int,
    sampleSize: Option[Int],
    logarithmic: Boolean)
  case class MetadataRequest(checkpoint: String, path: List[String])

  object GlobalSQLRequest extends FromJson[GlobalSQLRequest] {
    override def fromJson(j: json.JsValue) = j.as[GlobalSQLRequest]
  }
  case class GlobalSQLRequest(query: String, checkpoints: Map[String, String]) extends ViewRecipe {
    override def createDataFrame(
      user: User, context: SQLContext)(
        implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame = {
      val dfs = checkpoints.flatMap {
        case (name, cp) =>
          getDFsOfViewer(user, context, /* RemoteAPIController.getViewer(cp) */ ???, name)
      }
      DataManager.sql(context, query, dfs.toList)
    }

    /*
    // Lists all the DataFrames in the workspace.
    private def getDFsOfWorkspace(
      user: User,
      sqlContext: SQLContext,
      workspace: controllers.Workspace,
      prefix: String)(
        implicit mm: MetaGraphManager,
        dm: DataManager): Iterable[(String, DataFrame)] = {
      val leaves = workspace.leafStates
      val leafDFs =
        if (leaves.size == 1) getDFsOfViewer(user, sqlContext, leaves.head.project.viewer, prefix)
        else Seq()
      val fullPrefix = if (prefix.nonEmpty) prefix + "|" else ""
      leafDFs ++ workspace.states.flatMap(
        s => getDFsOfViewer(
          user, sqlContext, s.project.viewer, fullPrefix + s.box + "|" + s.output))
    }
    */

    // Lists all the DataFrames in the project/table/view given by the viewer.
    private def getDFsOfViewer(
      user: User,
      sqlContext: SQLContext,
      viewer: controllers.RootProjectViewer,
      prefix: String)(
        implicit mm: MetaGraphManager,
        dm: DataManager): Iterable[(String, DataFrame)] = {
      val fullPrefix = if (prefix.nonEmpty) prefix + "|" else ""
      val tableDFs = viewer.allRelativeTablePaths.flatMap { path =>
        val df = controllers.Table(path, viewer).toDF(sqlContext)
        Some((fullPrefix + path.toString) -> df) ++
          (if (path.toString == "vertices") Some(prefix -> df) else None)
      }
      val recipeDFs = viewer.viewRecipe.map(prefix -> _.createDataFrame(user, sqlContext))
      tableDFs ++ recipeDFs
    }
  }

  case class CheckpointRequest(checkpoint: String)
  case class TakeFromViewRequest(checkpoint: String, limit: Int)
  // Each row is a map, repeating the schema. Values may be missing for some rows.
  case class TableResult(rows: List[Map[String, json.JsValue]])
  case class DirectoryEntryRequest(
    path: String)
  case class DirectoryEntryResult(
    exists: Boolean,
    isView: Boolean,
    isTable: Boolean,
    isProject: Boolean,
    isDirectory: Boolean,
    isWorkspace: Boolean,
    isReadAllowed: Boolean,
    isWriteAllowed: Boolean)
  case class ExportViewToCSVRequest(
    checkpoint: String,
    path: String,
    header: Boolean,
    delimiter: String,
    quote: String,
    shufflePartitions: Option[Int])
  case class ExportViewToJsonRequest(checkpoint: String, path: String, shufflePartitions: Option[Int])
  case class ExportViewToORCRequest(checkpoint: String, path: String, shufflePartitions: Option[Int])
  case class ExportViewToParquetRequest(checkpoint: String, path: String, shufflePartitions: Option[Int])
  case class ExportViewToJdbcRequest(
    checkpoint: String, jdbcUrl: String, table: String, mode: String)
  case class ExportViewToTableRequest(checkpoint: String)
  case class PrefixedPathRequest(
    path: String)
  case class PrefixedPathResult(
    exists: Boolean, resolved: String)
  case class ListElement(name: String, checkpoint: String, objectType: String)
  case class ListResult(entries: List[ListElement])

  import WorkspaceJsonFormatters._
  implicit val wParquetMetadataResponse = json.Json.writes[ParquetMetadataResponse]
  implicit val wCheckpointResponse = json.Json.writes[CheckpointResponse]
  implicit val wOperationNamesResponse = json.Json.writes[OperationNamesResponse]
  implicit val rOperationRequest = json.Json.reads[OperationRequest]
  implicit val rLoadNameRequest = json.Json.reads[LoadNameRequest]
  implicit val rRemoveNameRequest = json.Json.reads[RemoveNameRequest]
  implicit val rSaveCheckpointRequest = json.Json.reads[SaveCheckpointRequest]
  implicit val rScalarRequest = json.Json.reads[ScalarRequest]
  implicit val rHistogramRequest = json.Json.reads[HistogramRequest]
  implicit val wHistogramResponse = json.Json.writes[HistogramResponse]
  implicit val rMetadataRequest = json.Json.reads[MetadataRequest]
  implicit val fGlobalSQLRequest = json.Json.format[GlobalSQLRequest]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wTableResult = json.Json.writes[TableResult]
  implicit val rCheckpointRequest = json.Json.reads[CheckpointRequest]
  implicit val rTakeFromViewRequest = json.Json.reads[TakeFromViewRequest]
  implicit val rDirectoryEntryRequest = json.Json.reads[DirectoryEntryRequest]
  implicit val wDirectoryEntryResult = json.Json.writes[DirectoryEntryResult]
  implicit val rExportViewToCSVRequest = json.Json.reads[ExportViewToCSVRequest]
  implicit val rExportViewToJsonRequest = json.Json.reads[ExportViewToJsonRequest]
  implicit val rExportViewToORCRequest = json.Json.reads[ExportViewToORCRequest]
  implicit val rExportViewToParquetRequest = json.Json.reads[ExportViewToParquetRequest]
  implicit val rExportViewToJdbcRequest = json.Json.reads[ExportViewToJdbcRequest]
  implicit val rExportViewToTableRequest = json.Json.reads[ExportViewToTableRequest]
  implicit val rPrefixedPathRequest = json.Json.reads[PrefixedPathRequest]
  implicit val wPrefixedPathResponse = json.Json.writes[PrefixedPathResult]
  implicit val wListElement = json.Json.writes[ListElement]
  implicit val wListResult = json.Json.writes[ListResult]

}

object RemoteAPIServer extends JsonServer {
  import RemoteAPIProtocol._
  val userController = ProductionJsonServer.userController
  val c = new RemoteAPIController(BigGraphProductionEnvironment)
  def getScalar = jsonFuturePost(c.getScalar)
  def getVertexHistogram = jsonFuturePost(c.getVertexHistogram)
  def getEdgeHistogram = jsonFuturePost(c.getEdgeHistogram)
  def getMetadata = jsonFuturePost(c.getMetadata)
  def getComplexView = jsonFuturePost(c.getComplexView)
  def getDirectoryEntry = jsonPost(c.getDirectoryEntry)
  def getPrefixedPath = jsonPost(c.getPrefixedPath)
  def getViewSchema = jsonPost(c.getViewSchema)
  def getParquetMetadata = jsonPost(c.getParquetMetadata)
  def newWorkspace = jsonPost(c.newWorkspace)
  def loadWorkspace = jsonPost(c.loadWorkspace)
  def removeName = jsonPost(c.removeName)
  def loadTable = jsonPost(c.loadTable)
  def loadView = jsonPost(c.loadView)
  def runOperation = jsonPost(c.runOperation)
  def getOperationNames = jsonPost(c.getOperationNames)
  def saveWorkspace = jsonPost(c.saveWorkspace)
  def saveTable = jsonPost(c.saveTable)
  def saveView = jsonPost(c.saveView)
  def takeFromView = jsonFuturePost(c.takeFromView)
  def exportViewToCSV = jsonFuturePost(c.exportViewToCSV)
  def exportViewToJson = jsonFuturePost(c.exportViewToJson)
  def exportViewToORC = jsonFuturePost(c.exportViewToORC)
  def exportViewToParquet = jsonFuturePost(c.exportViewToParquet)
  def exportViewToJdbc = jsonFuturePost(c.exportViewToJdbc)
  def exportViewToTable = jsonFuturePost(c.exportViewToTable)
  private def createView[T <: ViewRecipe: json.Writes: json.Reads] =
    jsonPost[T, CheckpointResponse](c.createView)
  def createViewJdbc = createView[JdbcImportRequest]
  def createViewHive = createView[HiveImportRequest]
  def createViewCSV = createView[CSVImportRequest]
  def createViewParquet = createView[ParquetImportRequest]
  def createViewORC = createView[ORCImportRequest]
  def createViewJson = createView[JsonImportRequest]
  def changeACL = jsonPost(c.changeACL)
  def globalSQL = createView[GlobalSQLRequest]
  def isComputed = jsonPost(c.isComputed)
  def computeWorkspace = jsonPost(c.computeWorkspace)
  def list = jsonPost(c.list)
  def cleanFileSystem = jsonPost(c.cleanFileSystem)
}

class RemoteAPIController(env: BigGraphEnvironment) {

  import RemoteAPIProtocol._

  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val ops = new frontend_operations.Operations(env)
  val sqlController = new SQLController(env)
  val bigGraphController = new BigGraphController(env)
  val graphDrawingController = new GraphDrawingController(env)

  def normalize(operation: String) = operation.replace(" ", "").toLowerCase
  def camelize(operation: String) = {
    val words = operation.split("-").toList
    val first = words.head.toLowerCase
    val rest = words.drop(1).map(_.toLowerCase.capitalize)
    first + rest.mkString("")
  }

  lazy val normalizedIds = ops.operationIds.map(id => normalize(id) -> id).toMap
  lazy val camelizedIds = ops.operationIds.map(id => camelize(id)).toList

  def getOperationNames(user: User, request: Empty): OperationNamesResponse = {
    OperationNamesResponse(camelizedIds)
  }

  def getBoxMetadatas(user: User, request: Empty): BoxMetadatasResponse = {
    BoxMetadatasResponse(ops.operationIds.toList.map(ops.getBoxMetadata(_)))
  }

  def getDirectoryEntry(user: User, request: DirectoryEntryRequest): DirectoryEntryResult = {
    val entry = new DirectoryEntry(
      SymbolPath.parse(request.path))
    DirectoryEntryResult(
      exists = entry.exists,
      isView = entry.isView,
      isTable = entry.isTable,
      isProject = entry.isProject,
      isDirectory = entry.isDirectory,
      isWorkspace = entry.isWorkspace,
      isReadAllowed = entry.readAllowedFrom(user),
      isWriteAllowed = entry.writeAllowedFrom(user)
    )
  }

  def getPrefixedPath(user: User, request: PrefixedPathRequest): PrefixedPathResult = {
    val file = HadoopFile(request.path)
    return PrefixedPathResult(
      exists = file.exists(),
      resolved = file.resolvedName)
  }

  def newWorkspace(user: User, request: Empty): CheckpointResponse = {
    CheckpointResponse("") // Blank checkpoint.
  }

  def loadObject(
    user: User,
    request: LoadNameRequest,
    validator: controllers.DirectoryEntry => Boolean): CheckpointResponse = {
    val entry = controllers.DirectoryEntry.fromName(request.name)
    assert(entry.exists, s"Entry '$entry' does not exist.")
    assert(validator(entry), s"Invalid frame type for '$entry'.")
    val frame = entry.asObjectFrame
    frame.assertReadAllowedFrom(user)
    CheckpointResponse(frame.checkpoint)
  }

  def loadWorkspace(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isWorkspace)
  }

  def loadTable(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isTable)
  }

  def loadView(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isView)
  }

  def removeName(
    user: User,
    request: RemoveNameRequest): Unit = {
    val entry = controllers.DirectoryEntry.fromName(request.name)
    assert(entry.exists, s"Entry '$entry' does not exist.")
    entry.remove()
  }

  def saveFrame(
    user: User,
    request: SaveCheckpointRequest,
    saver: (controllers.DirectoryEntry, String) => controllers.ObjectFrame): CheckpointResponse = {

    val entry = controllers.DirectoryEntry.fromName(request.name)
    entry.assertWriteAllowedFrom(user)
    val p = saver(entry, request.checkpoint)
    bigGraphController.changeACLSettings(
      user,
      ACLSettingsRequest(
        request.name,
        request.readACL.getOrElse(user.email),
        request.writeACL.getOrElse(user.email)))
    CheckpointResponse(p.checkpoint)
  }

  def saveWorkspace(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewWorkspaceFrame(_))
  }

  def saveTable(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewTableFrame(_))
  }

  def saveView(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewViewFrame(_))
  }

  def getViewer(cp: String): controllers.RootProjectViewer =
    new controllers.RootProjectViewer(metaManager.checkpointRepo.readCheckpoint(cp))

  /*
    val p = metaManager.checkpointRepo.readCheckpoint(cp)
    p.workspace.map { ws =>
      val leaves = ws.leafStates
      assert(leaves.size == 1, s"Workspace must have 1 leaf states: $ws")
      leaves.head.project.viewer
    }.getOrElse(new controllers.RootProjectViewer(p))
  }
  */

  // Get a viewer for a project which can be a root project or a segmentation.
  def getViewer(cp: String, path: List[String]): controllers.ProjectViewer = {
    val rootProjectViewer = getViewer(cp)
    rootProjectViewer.offspringViewer(path)
  }

  def getWorkspace(cp: String): controllers.Workspace =
    metaManager.checkpointRepo.readCheckpoint(cp).workspace.getOrElse(Workspace.empty)

  // Run an operation on a root project or a segmentation in a workspace.
  def runOperation(user: User, request: OperationRequest): CheckpointResponse = {
    ??? /*
    val normalized = normalize(request.operation)
    assert(normalizedIds.contains(normalized), s"No such operation: ${request.operation}")
    val operation = normalizedIds(normalized)
    val workspace = getWorkspace(request.checkpoint)
    val inputBoxes = request.inputs.values.map(c => workspace.findBox(c.box).get)
    val avgX = inputBoxes.map(_.x).sum / inputBoxes.size
    val avgY = inputBoxes.map(_.y).sum / inputBoxes.size
    val newBox = workspace.autoName(
      ops.getBoxMetadata(operation).toBox(request.parameters, x = avgX, y = avgY + 50))
    val newArrows = request.inputs.map { case (id, source) => Arrow(source, newBox.input(id)) }
    val newWorkspace = workspace.addBox(newBox).addArrows(newArrows)
    val checkpoint = newWorkspace.checkpoint(previous = request.checkpoint)
    CheckpointResponse(checkpoint)*/
  }

  def getScalar(user: User, request: ScalarRequest): Future[DynamicValue] = {
    val viewer = getViewer(request.checkpoint, request.path)
    val scalar = viewer.scalars(request.scalar)
    import dataManager.executionContext
    dataManager.getFuture(scalar).map(dynamicValue(_)).future
  }

  private def dynamicValue[T](scalar: ScalarData[T]) = {
    implicit val tt = scalar.typeTag
    DynamicValue.convert(scalar.value)
  }

  def getVertexHistogram(
    user: User,
    request: HistogramRequest): Future[HistogramResponse] = {
    val viewer = getViewer(request.checkpoint, request.path)
    val attributes = viewer.vertexAttributes
    assert(attributes.contains(request.attr), s"Vertex attribute '${request.attr}' does not exist.")
    histogram(user, viewer, attributes(request.attr), request)
  }

  def getEdgeHistogram(
    user: User,
    request: HistogramRequest): Future[HistogramResponse] = {
    val viewer = getViewer(request.checkpoint, request.path)
    val attributes = viewer.edgeAttributes
    assert(attributes.contains(request.attr), s"Edge attribute '${request.attr}' does not exist.")
    histogram(user, viewer, attributes(request.attr), request)
  }

  private def histogram(
    user: User,
    viewer: ProjectViewer,
    attr: Attribute[_],
    request: HistogramRequest): Future[HistogramResponse] = {
    assert(attr.is[String] || attr.is[Double], s"${request.attr}: is not String or Double")
    val requestSampleSize = request.sampleSize.getOrElse(-1)
    val req = HistogramSpec(
      attributeId = attr.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = request.numBuckets,
      axisOptions = AxisOptions(logarithmic = request.logarithmic),
      sampleSize = requestSampleSize)
    graphDrawingController.getHistogram(user, req)
  }

  // Only for testing.
  def getMetadata(user: User, request: MetadataRequest): Future[FEProject] = {
    val viewer = getViewer(request.checkpoint, request.path)
    import dataManager.executionContext
    Future(viewer.toFE(""))
  }

  def getComplexView(user: User, request: FEGraphRequest): Future[FEGraphResponse] = {
    val drawing = graphDrawingController
    import dataManager.executionContext
    Future(drawing.getComplexView(user, request))
  }

  private def dfToTableResult(df: org.apache.spark.sql.DataFrame, limit: Int) = {
    val schema = df.schema
    val data = if (limit >= 0) df.take(limit) else df.collect
    val rows = data.map { row =>
      schema.fields.zipWithIndex.flatMap {
        case (f, i) =>
          if (row.isNullAt(i)) None
          else {
            val jsValue = f.dataType match {
              case _: types.DoubleType => json.Json.toJson(row.getDouble(i))
              case _: types.StringType => json.Json.toJson(row.getString(i))
              case _: types.LongType => json.Json.toJson(row.getLong(i))
              case _ => json.Json.toJson(row.get(i).toString)
            }
            Some(f.name -> jsValue)
          }
      }.toMap
    }
    TableResult(rows = rows.toList)
  }

  def createView[T <: ViewRecipe: json.Writes](user: User, recipe: T): CheckpointResponse = {
    CheckpointResponse(ViewRecipe.saveAsCheckpoint(recipe))
  }

  def getViewSchema(user: User, request: CheckpointRequest): StructType = {
    val df = viewToDF(user, request.checkpoint)
    df.schema
  }

  private def viewToDF(user: User, checkpoint: String): DataFrame = {
    val viewer = getViewer(checkpoint)
    val sqlContext = SQLController.defaultContext(user)
    viewer.viewRecipe.get.createDataFrame(user, sqlContext)
  }

  def getParquetMetadata(user: User, request: PrefixedPathRequest): ParquetMetadataResponse = {
    val input = HadoopFile(request.path).resolvedName
    val conf = new Configuration()
    val inputPath = new Path(input)
    val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
    val footers = ParquetFileReader.readFooters(conf, inputFileStatus, false)

    ParquetMetadataResponse(
      footers.asScala.flatMap { f =>
        val blocks = f.getParquetMetadata().getBlocks().asScala
        blocks.map(_.getRowCount())
      }.sum
    )
  }

  def takeFromView(
    user: User, request: TakeFromViewRequest): Future[TableResult] = dataManager.async {
    val df = viewToDF(user, request.checkpoint)
    dfToTableResult(df, request.limit)
  }

  def exportViewToCSV(user: User, request: ExportViewToCSVRequest) = {
    exportViewToFile(
      user, request.checkpoint, request.path, "csv",
      request.shufflePartitions,
      Map(
        "delimiter" -> request.delimiter,
        "quote" -> request.quote,
        "nullValue" -> "",
        "header" -> (if (request.header) "true" else "false")))
  }

  def exportViewToJson(user: User, request: ExportViewToJsonRequest) = {
    exportViewToFile(user, request.checkpoint, request.path, "json", request.shufflePartitions)
  }

  def exportViewToORC(user: User, request: ExportViewToORCRequest) = {
    exportViewToFile(user, request.checkpoint, request.path, "orc", request.shufflePartitions)
  }

  def exportViewToParquet(user: User, request: ExportViewToParquetRequest) = {
    exportViewToFile(user, request.checkpoint, request.path, "parquet", request.shufflePartitions)
  }

  def exportViewToJdbc(
    user: User, request: ExportViewToJdbcRequest): Future[Unit] = dataManager.async {
    val df = viewToDF(user, request.checkpoint)
    df.write.mode(request.mode).jdbc(request.jdbcUrl, request.table, new java.util.Properties)
  }

  private def exportViewToFile(
    user: serving.User,
    checkpoint: String,
    path: String,
    format: String,
    shufflePartitions: Option[Int],
    options: Map[String, String] = Map()): Future[Unit] = dataManager.async {
    val file = HadoopFile(path)
    file.assertWriteAllowedFrom(user)
    val viewDF = viewToDF(user, checkpoint)
    val df =
      if (shufflePartitions.isEmpty) viewDF
      else viewDF.coalesce(shufflePartitions.get)
    for (sp <- shufflePartitions) {
      df.sqlContext.setConf("spark.sql.shuffle.partitions", sp.toString)
    }
    df.write.mode(SaveMode.Overwrite).format(format).options(options).save(file.resolvedName)
  }

  def exportViewToTable(
    user: User, request: ExportViewToTableRequest): Future[CheckpointResponse] = dataManager.async {
    val df = viewToDF(user, request.checkpoint)
    val table = TableImport.importDataFrameAsync(df)
    val cp = table.saveAsCheckpoint("Created from a view via the Remote API.")
    CheckpointResponse(cp)
  }

  private def isComputed(entity: TypedEntity[_]): Boolean = {
    val progress = dataManager.computeProgress(entity)
    progress >= 1.0
  }

  // Checks Whether all the scalars, attributes and segmentations of the project are already computed.
  def isComputed(user: User, request: CheckpointRequest): Boolean = {
    val editor = getViewer(request.checkpoint).editor
    isComputed(editor)
  }

  private def isComputed(editor: ProjectEditor): Boolean = {
    val scalars = editor.scalars.iterator.map { case (name, scalar) => scalar }
    val attributes = (editor.vertexAttributes ++ editor.edgeAttributes).values
    val segmentations = editor.viewer.sortedSegmentations

    scalars.forall(isComputed) &&
      attributes.forall(isComputed) &&
      segmentations.map(_.editor).forall(isComputed)
  }

  def computeWorkspace(user: User, request: CheckpointRequest): Unit = {
    val viewer = getViewer(request.checkpoint)
    computeProject(viewer.editor)
  }

  private def computeProject(editor: ProjectEditor): Unit = {
    for ((name, scalar) <- editor.scalars) {
      dataManager.get(scalar)
    }

    val attributes = editor.vertexAttributes ++ editor.edgeAttributes
    for ((name, attr) <- attributes) {
      dataManager.get(attr)
    }

    val segmentations = editor.viewer.sortedSegmentations
    segmentations.foreach(s => computeProject(s.editor))
  }

  def changeACL(user: User, request: ACLSettingsRequest) = {
    bigGraphController.changeACLSettings(user, request)
  }

  def list(user: User, request: ProjectListRequest) = {
    val list = bigGraphController.projectList(user, request)
    ListResult(
      list.directories.map(d => ListElement(d, "", "directory")) ++
        list.objects.map(e =>
          ListElement(
            e.name,
            controllers.DirectoryEntry.fromName(e.name).asObjectFrame.checkpoint,
            e.objectType))
    )
  }

  def cleanFileSystem(user: User, request: Empty) = {
    val cleanerController = new CleanerController(env)
    cleanerController.moveAllToCleanerTrash(user)
    cleanerController.emptyCleanerTrash(user, request)
  }
}
