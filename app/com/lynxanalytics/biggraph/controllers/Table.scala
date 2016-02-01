// Core classes for representing columnar data using meta graph entities.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.table._

import org.apache.spark
import org.apache.spark.sql.SQLContext

trait Table {
  def idSet: VertexSet
  def columns: Map[String, Attribute[_]]

  def toDF(sqlContext: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame =
    new TableRelation(this, sqlContext).toDF

  def dataFrameSchema: spark.sql.types.StructType = {
    val fields = columns.toSeq.sortBy(_._1).map {
      case (name, attr) =>
        spark.sql.types.StructField(
          name = name,
          dataType = spark.sql.catalyst.ScalaReflection.schemaFor(attr.typeTag).dataType)
    }
    spark.sql.types.StructType(fields)
  }

  def saveAsCheckpoint(notes: String)(
    implicit manager: MetaGraphManager): String = manager.synchronized {

    val editor = new RootProjectEditor(RootProjectState.emptyState)
    editor.notes = notes
    editor.vertexSet = idSet
    for ((name, attr) <- columns) {
      editor.vertexAttributes(name) = attr
    }
    val checkpointedState =
      manager.checkpointRepo.checkpointState(editor.rootState, prevCheckpoint = "")
    checkpointedState.checkpoint.get
  }
}
object Table {
  val VertexTableName = "vertices"
  val EdgeTableName = "edges"
  val TripletTableName = "triplets"
  val BelongsToTableName = "belongsTo"
  val ReservedTableNames =
    Set(VertexTableName, EdgeTableName, TripletTableName, BelongsToTableName)

  // A canonical table path is what's used by operations to reference a table. It's always meant to
  // be a valid id for an FEOption.
  // A canonical table path can be in one of the follow formats:
  // 1. sub-project relative reference:
  //    RELATIVE_REFERENCE := (SEGMENTATION_NAME|)*TABLE_NAME
  //    e.g.: connected components|vertices
  // 2. project frame relative reference:
  //    ABSOLUTE_REFERENCE := |RELATIVE_REFERENCE
  //    e.g.: |events|connected components|edges
  // 3. global reference:
  //    !checkpoint(CHECKPOINT,CHECKPOINT_NOTE)ABSOLUTE_REFERENCE
  //    e.g.: !checkpoint(1234,"My Favorite Project")|vertices
  //
  // TABLE_NAME either points to a user defined table within the root project/segmentation
  // or can be one of the following implicitly defined tables:
  //  vertices
  //  edges
  //  triplets
  //  belongsTo - only defined for segmentations
  //
  // The first two formats are only meaningful in the context of a project viewer. Global paths
  // can be resolved out of context as well, so there is a specialized function just for those.
  def fromGlobalPath(globalPath: String)(implicit metaManager: MetaGraphManager): Table = {
    val (checkpoint, _, suffix) = FEOption.unpackTitledCheckpoint(
      globalPath,
      s"$globalPath does not seem to be a valid global table path")
    fromCheckpointAndPath(checkpoint, suffix)
  }

  def fromCanonicalPath(path: String, context: ProjectViewer)(
    implicit metaManager: MetaGraphManager): Table = {

    FEOption.maybeUnpackTitledCheckpoint(path)
      .map { case (checkpoint, _, suffix) => fromCheckpointAndPath(checkpoint, suffix) }
      .getOrElse(fromPath(path, context))
  }

  def fromTableName(tableName: String, viewer: ProjectViewer): Table = {
    tableName match {
      case VertexTableName => new VertexTable(viewer)
      case EdgeTableName => new EdgeTable(viewer)
      case TripletTableName => new TripletTable(viewer)
      case BelongsToTableName => {
        assert(
          viewer.isInstanceOf[SegmentationViewer],
          s"The $BelongsToTableName table is only defined on segmentations.")
        new BelongsToTable(viewer.asInstanceOf[SegmentationViewer])
      }
      case customTableName: String => {
        println(customTableName)
        ???
      }
    }
  }

  def fromRelativePath(relativePath: String, viewer: ProjectViewer): Table = {
    val splitPath = SubProject.splitPipedPath(relativePath)
    fromTableName(splitPath.last, viewer.offspringViewer(splitPath.dropRight(1)))
  }

  def fromPath(path: String, viewer: ProjectViewer): Table = {
    if (path(0) == '|') {
      fromRelativePath(path.drop(1), viewer.rootViewer)
    } else {
      fromRelativePath(path, viewer)
    }
  }

  def fromCheckpointAndPath(checkpoint: String, path: String)(
    implicit manager: MetaGraphManager): Table = {
    val rootViewer = new RootProjectViewer(manager.checkpointRepo.readCheckpoint(checkpoint))
    fromPath(path, rootViewer)
  }
}

case class RawTable(idSet: VertexSet, columns: Map[String, Attribute[_]]) extends Table

class VertexTable(project: ProjectViewer) extends Table {
  assert(project.vertexSet != null, "Cannot define a VertexTable on a project w/o vertices")

  def idSet = project.vertexSet
  def columns = project.vertexAttributes
}

class EdgeTable(project: ProjectViewer) extends Table {
  assert(project.edgeBundle != null, "Cannot define an EdgeTable on a project w/o edges")

  def idSet = project.edgeBundle.idSet
  def columns = project.edgeAttributes
}

class TripletTable(project: ProjectViewer) extends Table {
  assert(project.edgeBundle != null, "Cannot define an EdgeTable on a project w/o edges")

  def idSet = project.edgeBundle.idSet
  def columns = {
    import graph_operations.VertexToEdgeAttribute._
    implicit val metaManager = project.vertexSet.source.manager
    val edgeAttrs = project.edgeAttributes.map {
      case (name, attr) => s"edge_$name" -> attr
    }.toMap[String, Attribute[_]]
    val srcAttrs = project.vertexAttributes.map {
      case (name, attr) => s"src_$name" -> srcAttribute(attr, project.edgeBundle)
    }.toMap[String, Attribute[_]]
    val dstAttrs = project.vertexAttributes.map {
      case (name, attr) => s"dst_$name" -> dstAttribute(attr, project.edgeBundle)
    }.toMap[String, Attribute[_]]
    edgeAttrs ++ srcAttrs ++ dstAttrs
  }
}

class BelongsToTable(segmentation: SegmentationViewer) extends Table {
  def idSet = segmentation.belongsTo.idSet
  def columns = {
    import graph_operations.VertexToEdgeAttribute._
    implicit val metaManager = segmentation.vertexSet.source.manager
    val baseProjectAttributes = segmentation.parent.vertexAttributes.map {
      case (name, attr) => s"base_$name" -> srcAttribute(attr, segmentation.belongsTo)
    }.toMap[String, Attribute[_]]
    val segmentationAttributes = segmentation.vertexAttributes.map {
      case (name, attr) => s"segment_$name" -> dstAttribute(attr, segmentation.belongsTo)
    }.toMap[String, Attribute[_]]
    baseProjectAttributes ++ segmentationAttributes
  }
}
