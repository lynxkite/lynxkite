// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import scala.concurrent.Future
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

object RemoteAPIProtocol {
  case class ParquetMetadataResponse(rowCount: Long)
  case class OperationNamesResponse(names: List[String])
  case class BoxMetadatasResponse(boxes: List[BoxMetadata])
  case class LoadNameRequest(name: String)
  case class RemoveNameRequest(name: String, force: Boolean)

  case class TableResult(rows: List[Map[String, json.JsValue]])
  case class DirectoryEntryRequest(
      path: String)
  case class DirectoryEntryResult(
      exists: Boolean,
      isDirectory: Boolean,
      isWorkspace: Boolean,
      isSnapshot: Boolean,
      isReadAllowed: Boolean,
      isWriteAllowed: Boolean)
  case class PrefixedPathRequest(
      path: String)
  case class PrefixedPathResult(
      exists: Boolean,
      resolved: String)
  case class ListElement(name: String, checkpoint: String, objectType: String)
  case class ListResult(entries: List[ListElement])
  case class SetExecutorsRequest(count: Int)

  import WorkspaceJsonFormatters._
  implicit val wParquetMetadataResponse = json.Json.writes[ParquetMetadataResponse]
  implicit val wOperationNamesResponse = json.Json.writes[OperationNamesResponse]
  implicit val rLoadNameRequest = json.Json.reads[LoadNameRequest]
  implicit val rRemoveNameRequest = json.Json.reads[RemoveNameRequest]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wTableResult = json.Json.writes[TableResult]
  implicit val rDirectoryEntryRequest = json.Json.reads[DirectoryEntryRequest]
  implicit val wDirectoryEntryResult = json.Json.writes[DirectoryEntryResult]
  implicit val rPrefixedPathRequest = json.Json.reads[PrefixedPathRequest]
  implicit val wPrefixedPathResponse = json.Json.writes[PrefixedPathResult]
  implicit val wListElement = json.Json.writes[ListElement]
  implicit val wListResult = json.Json.writes[ListResult]
  implicit val rSetExecutorsRequest = json.Json.reads[SetExecutorsRequest]
}

class RemoteAPIServer @javax.inject.Inject() (
    implicit
    ec: concurrent.ExecutionContext,
    fmt: play.api.http.FileMimeTypes,
    cfg: play.api.Configuration,
    cc: play.api.mvc.ControllerComponents,
    pjs: ProductionJsonServer)
    extends JsonServer {
  import RemoteAPIProtocol._
  val userController = ProductionJsonServer.userController
  val c = new RemoteAPIController(BigGraphProductionEnvironment, pjs)
  def getDirectoryEntry = jsonPost(c.getDirectoryEntry)
  def getPrefixedPath = jsonPost(c.getPrefixedPath)
  def getParquetMetadata = jsonPost(c.getParquetMetadata)
  def removeName = jsonPost(c.removeName)
  def getOperationNames = jsonPost(c.getOperationNames)
  def changeACL = jsonPost(c.changeACL)
  def list = jsonPost(c.list)
  def cleanFileSystem = jsonPost(c.cleanFileSystem)
  def setExecutors = jsonPost(c.setExecutors)
}

class RemoteAPIController(env: BigGraphEnvironment, pjs: ProductionJsonServer) {

  import RemoteAPIProtocol._

  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val ops = new frontend_operations.Operations(env)
  val bigGraphController = new BigGraphController(env)
  val graphDrawingController = new GraphDrawingController(env)

  def normalize(operation: String) = operation.replace(" ", "").toLowerCase
  def camelize(operation: String) = {
    val words = operation.split("-").toList
    val first = words.head.toLowerCase
    val rest = words.drop(1).map(_.toLowerCase.capitalize)
    first + rest.mkString("")
  }

  lazy val normalizedIds = ops.atomicOperationIds.map(id => normalize(id) -> id).toMap
  lazy val camelizedIds = ops.atomicOperationIds.map(id => camelize(id)).toList

  def getOperationNames(user: User, request: Empty): OperationNamesResponse = {
    OperationNamesResponse(camelizedIds)
  }

  def getBoxMetadatas(user: User, request: Empty): BoxMetadatasResponse = {
    BoxMetadatasResponse(ops.atomicOperationIds.toList.map(ops.getBoxMetadata(_)))
  }

  def getDirectoryEntry(user: User, request: DirectoryEntryRequest): DirectoryEntryResult = {
    val entry = new DirectoryEntry(
      SymbolPath.parse(request.path))
    DirectoryEntryResult(
      exists = entry.exists,
      isDirectory = entry.isDirectory,
      isWorkspace = entry.isWorkspace,
      isSnapshot = entry.isSnapshot,
      isReadAllowed = entry.readAllowedFrom(user),
      isWriteAllowed = entry.writeAllowedFrom(user),
    )
  }

  def getPrefixedPath(user: User, request: PrefixedPathRequest): PrefixedPathResult = {
    val file = HadoopFile(request.path)
    return PrefixedPathResult(
      exists = file.exists(),
      resolved = file.resolvedName)
  }

  def removeName(
      user: User,
      request: RemoveNameRequest): Unit = {
    val entry = controllers.DirectoryEntry.fromName(request.name)
    if (!request.force) {
      assert(entry.exists, s"Entry '$entry' does not exist.")
    }
    entry.remove()
  }

  def getComplexView(user: User, request: FEGraphRequest): Future[FEGraphResponse] = {
    val drawing = graphDrawingController
    val ec = env.sparkDomain.executionContext // TODO: Revise when we have single-node drawing.
    Future(drawing.getComplexView(user, request))(ec)
  }

  def getParquetMetadata(user: User, request: PrefixedPathRequest): ParquetMetadataResponse = {
    val input = HadoopFile(request.path).resolvedName
    val conf = new Configuration()
    val inputPath = new Path(input)
    val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
    val inputFile = org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(inputFileStatus, conf)
    val reader = ParquetFileReader.open(inputFile)
    try {
      val blocks = reader.getFooter.getBlocks.asScala
      ParquetMetadataResponse(rowCount = blocks.map(_.getRowCount).sum)
    } finally {
      reader.close()
    }
  }

  def changeACL(user: User, request: ACLSettingsRequest) = {
    bigGraphController.changeACLSettings(user, request)
  }

  def list(user: User, request: EntryListRequest) = {
    val list = bigGraphController.entryList(user, request)
    ListResult(
      list.directories.map(d => ListElement(d, "", "directory")) ++
        list.objects.map(e =>
          ListElement(
            e.name,
            controllers.DirectoryEntry.fromName(e.name).asObjectFrame.checkpoint,
            e.objectType)))
  }

  def cleanFileSystem(user: User, request: Empty) = {
    val cleanerController = pjs.cleanerController
    cleanerController.moveAllToCleanerTrash(user)
    cleanerController.emptyCleanerTrash(user, request)
  }

  def setExecutors(user: User, request: SetExecutorsRequest) = {
    assert(user.isAdmin, "Only administrator users can set the number of executors.")
    env.sparkContext.requestTotalExecutors(
      request.count,
      localityAwareTasks = 0,
      hostToLocalTaskCount = Map.empty)
  }
}
