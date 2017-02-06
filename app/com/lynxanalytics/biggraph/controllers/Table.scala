// Core classes for representing columnar data using meta graph entities.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.table._

import scala.reflect.runtime.universe._
import org.apache.spark
import org.apache.spark.sql.SQLContext

trait Table {
  def idSet: VertexSet
  def columns: Map[String, Attribute[_]]
  def column(name: String): Attribute[_] = {
    lazy val colNames = columns.keys.mkString(", ")
    assert(columns.contains(name), s"Invalid table column ${name}. Possible values: ${colNames}.")
    columns(name)
  }

  def toDF(sqlContext: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame =
    new TableRelation(this, sqlContext, dataManager).toDF

  def dataFrameSchema: spark.sql.types.StructType = {
    val fields = columns.toSeq.sortBy(_._1).map {
      case (name, attr) =>
        spark.sql.types.StructField(
          name = name,
          dataType = Table.dfType(attr.typeTag))
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

  // Returns the RDD for the attribute, if it is a supported DataFrame type.
  // Unsupported types are converted to string.
  def columnForDF(name: String)(implicit dm: DataManager): AttributeRDD[_] = {
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    val col = columns(name)
    if (Table.supportedDFType(col.typeTag).isDefined) col.rdd
    else col.rdd.mapValues(_.toString)
  }
}
object Table {
  val VertexTableName = "vertices"
  val EdgeTableName = "edges"
  val EdgeAttributeTableName = "edge_attributes"
  val BelongsToTableName = "belongs_to"
  val ReservedTableNames =
    Set(VertexTableName, EdgeTableName, EdgeAttributeTableName, BelongsToTableName)

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
  //  edge_attributes
  //  belongs_to - only defined for segmentations
  //
  // The first two formats are only meaningful in the context of a project viewer. Global paths
  // can be resolved out of context as well, so there is a specialized function just for those.
  def apply(path: TablePath, context: ProjectViewer)(implicit m: MetaGraphManager): Table = {
    fromTableName(path.tableName, path.containingViewer(context))
  }
  def apply(path: RelativeTablePath, context: ProjectViewer): Table = {
    fromTableName(path.tableName, path.containingViewerOfRelativeTablePath(context))
  }
  def apply(path: GlobalTablePath)(implicit m: MetaGraphManager): Table = {
    fromTableName(path.tableName, path.containingViewer)
  }

  def fromTableName(tableName: String, viewer: ProjectViewer): Table = {
    tableName match {
      case VertexTableName => new VertexTable(viewer)
      case EdgeTableName => new EdgeTable(viewer)
      case EdgeAttributeTableName => new EdgeAttributeTable(viewer)
      case BelongsToTableName =>
        assert(
          viewer.isInstanceOf[SegmentationViewer],
          s"The $BelongsToTableName table is only defined on segmentations.")
        new BelongsToTable(viewer.asInstanceOf[SegmentationViewer])
      case customTableName: String =>
        throw new AssertionError(s"Table $customTableName not found.")
    }
  }

  private def supportedDFType[T: TypeTag]: Option[spark.sql.types.DataType] = {
    try {
      Some(spark.sql.catalyst.ScalaReflection.schemaFor(typeTag[T]).dataType)
    } catch {
      case _: UnsupportedOperationException => None
    }
  }

  def dfType[T: TypeTag]: spark.sql.types.DataType = {
    // Convert unsupported types to string.
    supportedDFType[T].getOrElse(spark.sql.types.StringType)
  }
}

sealed trait TablePath {
  def toFE: FEOption
  // Returns the final ProjectViewer which directly contains the table.
  def containingViewer(viewer: ProjectViewer)(implicit manager: MetaGraphManager): ProjectViewer
  def tableName: String
}
object TablePath {
  def parse(path: String): TablePath = {
    assert(path.nonEmpty, "Empty table path.")
    val g = GlobalTablePath.maybeParse(path)
    if (g.nonEmpty) g.get
    else if (path.head == '|') AbsoluteTablePath(split(path.tail))
    else RelativeTablePath(split(path))
  }

  def split(path: String) = SubProject.splitPipedPath(path)
}

case class RelativeTablePath(path: Seq[String]) extends TablePath {
  override def toString = path.mkString("|")
  def toFE = FEOption.regular(toString)
  def tableName = path.last

  def containingViewerOfRelativeTablePath(viewer: ProjectViewer) = {
    viewer.offspringViewer(path.init)
  }

  def containingViewer(viewer: ProjectViewer)(implicit manager: MetaGraphManager) = {
    containingViewerOfRelativeTablePath(viewer)
  }

  def toAbsolute(prefix: Seq[String]) = AbsoluteTablePath(prefix ++ path)

  def /:(prefix: String) = RelativeTablePath(prefix +: path)
}

case class AbsoluteTablePath(path: Seq[String]) extends TablePath {
  override def toString = "|" + RelativeTablePath(path).toString
  def toFE = FEOption.regular(toString)
  def tableName = RelativeTablePath(path).tableName

  def containingViewer(viewer: ProjectViewer)(implicit manager: MetaGraphManager) = {
    RelativeTablePath(path).containingViewer(viewer.rootViewer)
  }

  def toGlobal(checkpoint: String, name: String) = GlobalTablePath(checkpoint, name, path)
}

case class GlobalTablePath(checkpoint: String, name: String, path: Seq[String]) extends TablePath {
  override def toString = toFE.id
  def toFE = FEOption.titledCheckpoint(checkpoint, name, AbsoluteTablePath(path).toString)
  def tableName = AbsoluteTablePath(path).tableName

  def containingViewer(viewer: ProjectViewer)(implicit manager: MetaGraphManager) = containingViewer

  def containingViewer()(implicit manager: MetaGraphManager): ProjectViewer = {
    val rootViewer = new RootProjectViewer(manager.checkpointRepo.readCheckpoint(checkpoint))
    AbsoluteTablePath(path).containingViewer(rootViewer)
  }
}
object GlobalTablePath {
  def parse(path: String): GlobalTablePath = {
    val p = maybeParse(path)
    assert(p.nonEmpty, s"$path does not seem to be a valid global table path.")
    p.get
  }

  def maybeParse(path: String): Option[GlobalTablePath] = {
    FEOption.maybeUnpackTitledCheckpoint(path).map {
      case (checkpoint, name, suffix) =>
        GlobalTablePath(checkpoint, name, TablePath.split(suffix.tail))
    }
  }
}

case class RawTable(idSet: VertexSet, columns: Map[String, Attribute[_]]) extends Table

class VertexTable(project: ProjectViewer) extends Table {
  assert(project.vertexSet != null, "Cannot define a VertexTable on a project w/o vertices")

  def idSet = project.vertexSet
  def columns = project.vertexAttributes
}

class EdgeAttributeTable(project: ProjectViewer) extends Table {
  assert(project.edgeBundle != null, "Cannot define an EdgeAttributeTable on a project w/o edges")

  def idSet = project.edgeBundle.idSet
  def columns = project.edgeAttributes
}

class EdgeTable(project: ProjectViewer) extends Table {
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
