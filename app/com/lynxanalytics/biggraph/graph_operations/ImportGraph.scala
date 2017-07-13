// Operations and other classes for importing data in general and from CSV files.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.JavaScriptEvaluator
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.protection.Limitations
import com.lynxanalytics.biggraph.spark_util.HybridRDD
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import org.apache.commons.lang.StringEscapeUtils

// Functions for looking at CSV files. The frontend can use these when
// constructing the import operation.
@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportUtil {
  def header(file: HadoopFile): String = {
    // TODO: we don't check here if all the files begin with the same header!
    val files = file.list
    assert(files.nonEmpty, s"Not found: $file")
    // Read from first file if there is a glob.
    files.head.readFirstLine()
  }

  private[graph_operations] def splitter(delimiter: String): String => Seq[String] = {
    val delim = java.util.regex.Pattern.quote(delimiter)
    def oneOf(options: String*) = options.mkString(".")
    def any(p: String) = capture(p) + "*"
    def capture(p: String) = "(" + p + ")"
    def oneField(p: String) = oneOf(capture(p + delim), capture(p + "$")) // Delimiter or line end.
    val quote = "\""
    val nonQuote = "[^\"]"
    val doubleQuote = quote + quote
    val quotedString = quote + any(oneOf(nonQuote, doubleQuote)) + quote
    val anyString = ".*?"
    val r = oneOf(oneField(quotedString), oneField(anyString)).r
    val splitter = { line: String =>
      val matches = r.findAllMatchIn(line)
      // Find the top-level group that has matched in each field.
      val fields = matches.map(_.subgroups.find(_ != null).get).toList
      // The regex will always have an empty match at the end, which we may or may not need to
      // include. We include all the matches that end with a comma, plus one that does not.
      val lastIndex = fields.indexWhere(!_.endsWith(delimiter))
      val fixed = fields.take(lastIndex + 1).map(_.stripSuffix(delimiter))
      // Remove quotes and unescape double-quotes in quoted fields.
      fixed.map { field =>
        if (field.startsWith(quote) && field.endsWith(quote)) {
          field.slice(1, field.length - 1).replace(doubleQuote, quote)
        } else field
      }
    }
    return splitter
  }

  private val splitters = collection.mutable.Map[String, String => Seq[String]]()

  // Splits a line by the delimiter. Delimiters inside quoted fields are ignored. (They become part
  // of the string.) Quotes inside quoted fields must be escaped by doubling them (" -> "").
  // TODO: Maybe we should use a CSV library.
  private[graph_operations] def split(line: String, delimiter: String): Seq[String] = {
    // Cache the regular expressions.
    if (!splitters.contains(delimiter)) {
      splitters(delimiter) = splitter(delimiter)
    }
    return splitters(delimiter)(line)
  }
}

@deprecated("Replaced by table-based importing.", "1.7.0")
trait RowInput extends ToJson {
  def fields: Seq[String]
  def lines(rc: RuntimeContext): UniqueSortedRDD[ID, Seq[String]]
  val mayHaveNulls: Boolean
}

@deprecated("Replaced by table-based importing.", "1.7.0")
object CSV extends FromJson[CSV] {
  private val omitFieldsParameter = NewParameter("omitFields", Set[String]())
  private val allowCorruptLinesParameter = NewParameter("allowCorruptLines", true)
  def fromJson(j: JsValue): CSV = {
    val header = (j \ "header").as[String]
    val delimiter = (j \ "delimiter").as[String]
    val omitFields = omitFieldsParameter.fromJson(j)
    val allowCorruptLines = allowCorruptLinesParameter.fromJson(j)
    val fields = getFields(delimiter, header)
    new CSV(
      HadoopFile((j \ "file").as[String], true),
      delimiter,
      header,
      fields,
      omitFields,
      JavaScript((j \ "filter").as[String]),
      allowCorruptLines)
  }

  def getFields(delimiter: String, header: String): Seq[String] = {
    val unescapedDelimiter = StringEscapeUtils.unescapeJava(delimiter)
    ImportUtil.split(header, unescapedDelimiter).map(_.trim)
  }

  def apply(file: HadoopFile,
            delimiter: String,
            header: String,
            omitFields: Set[String] = Set(),
            filter: JavaScript = JavaScript(""),
            allowCorruptLines: Boolean = true): CSV = {
    val fields = getFields(delimiter, header)
    assert(
      fields.forall(_.nonEmpty),
      s"CSV column with empty name is not allowed. Column names were: $fields")
    assert(
      (fields.toSet.size == fields.size),
      s"Duplicate CSV column name is not allowed. Column names were: $fields")
    assert(file.list.nonEmpty, s"$file does not exist.")
    assert(
      omitFields.forall(fields.contains(_)),
      {
        val missingColumns = omitFields.filter(!fields.contains(_)).mkString(", ")
        s"Column(s) $missingColumns that you asked to omit are not actually columns."
      })
    new CSV(file, delimiter, header, fields, omitFields, filter, allowCorruptLines)
  }
}
@deprecated("Replaced by table-based importing.", "1.7.0")
case class CSV private (file: HadoopFile,
                        delimiter: String,
                        header: String,
                        allFields: Seq[String],
                        omitFields: Set[String],
                        filter: JavaScript,
                        allowCorruptLines: Boolean) extends RowInput {
  val unescapedDelimiter = StringEscapeUtils.unescapeJava(delimiter)
  val fields = allFields.filter(field => !omitFields.contains(field))
  override def toJson = {
    Json.obj(
      "file" -> file.symbolicName,
      "delimiter" -> delimiter,
      "header" -> header,
      "filter" -> filter.expression) ++
      CSV.omitFieldsParameter.toJson(omitFields) ++
      CSV.allowCorruptLinesParameter.toJson(allowCorruptLines)
  }

  def lines(rc: RuntimeContext): UniqueSortedRDD[ID, Seq[String]] = {
    val lines = file.loadTextFile(rc.sparkContext)
    val numRows = lines.count()
    val partitioner = rc.partitionerForNRows(numRows)
    val numPartitions = partitioner.numPartitions
    log.info(s"Reading $file ($numRows lines) into $numPartitions partitions.")

    val fullRows = lines
      .filter(_ != header)
      .map(ImportUtil.split(_, unescapedDelimiter))
      .filter(checkNumberOfFields(_))
      .mapPartitions({ it =>
        val evaluator = filter.evaluator
        it.filter(jsFilter(_, evaluator))
      }, preservesPartitioning = true)
    val keptFields = if (omitFields.nonEmpty) {
      val keptIndices = allFields.zipWithIndex.filter(x => !omitFields.contains(x._1)).map(_._2)
      fullRows.map(fullRow => keptIndices.map(idx => fullRow(idx)))
    } else {
      fullRows
    }
    keptFields.randomNumbered(partitioner)
  }

  val mayHaveNulls = false

  private def jsFilter(line: Seq[String], evaluator: JavaScriptEvaluator): Boolean = {
    if (filter.isEmpty) {
      true
    } else {
      evaluator.evaluateBoolean(allFields.zip(line).toMap).getOrElse(false)
    }
  }

  private def checkNumberOfFields(line: Seq[String]): Boolean = {
    if (line.length != allFields.length) {
      val msg =
        s"Input cannot be parsed: $line (contains ${line.length} fields, " +
          s"should be: ${allFields.length})"
      log.info(msg)
      assert(allowCorruptLines, msg +
        " You can set parameter 'Tolerate ill-formed lines' to 'yes' to skip all such lines.")
      return false;
    }
    return true;
  }
}

@deprecated("Replaced by table-based importing.", "1.7.0")
trait ImportCommon {
  class Columns(
      allNumberedLines: UniqueSortedRDD[ID, Seq[String]],
      fields: Seq[String],
      mayHaveNulls: Boolean,
      requiredFields: Set[String] = Set()) {
    val numberedValidLines =
      if (requiredFields.isEmpty || !mayHaveNulls) allNumberedLines
      else {
        val requiredIndexes = requiredFields.map(fieldName => fields.indexOf(fieldName)).toArray
        allNumberedLines.filter {
          case (id, line) => requiredIndexes.forall(idx => line(idx) != null)
        }
      }
    val singleColumns = fields.zipWithIndex.map {
      case (field, idx) =>
        (field,
          if (mayHaveNulls) numberedValidLines.flatMapOptionalValues(line => Option(line(idx)))
          else numberedValidLines.mapValues(line => line(idx)))
    }.toMap

    def apply(fieldName: String) = singleColumns(fieldName)

    def columnPair(fieldName1: String, fieldName2: String): UniqueSortedRDD[ID, (String, String)] = {
      val idx1 = fields.indexOf(fieldName1)
      val idx2 = fields.indexOf(fieldName2)
      if (mayHaveNulls) {
        numberedValidLines.flatMapOptionalValues { line =>
          val value1 = line(idx1)
          val value2 = line(idx2)
          if ((value1 != null) && (value2 != null)) Some((value1, value2))
          else None
        }
      } else {
        numberedValidLines.mapValues(line => (line(idx1), line(idx2)))
      }
    }
  }

  val input: RowInput

  protected def mustHaveField(field: String) = {
    assert(input.fields.contains(field), s"No such field: $field in ${input.fields}")
  }

  protected def numberedLines(
    rc: RuntimeContext,
    input: RowInput): UniqueSortedRDD[ID, Seq[String]] = {
    val numbered = input.lines(rc).copyWithAncestorsCached
    val maxLines = Limitations.maxImportedLines
    if (maxLines >= 0) {
      val numLines = numbered.count
      if (numLines > maxLines) {
        throw new AssertionError(
          s"Can't import $numLines lines as your licence only allows $maxLines.")
      }
    }
    numbered
  }

  protected def readColumns(
    rc: RuntimeContext,
    input: RowInput,
    requiredFields: Set[String] = Set()): Columns = {
    return new Columns(numberedLines(rc, input), input.fields, input.mayHaveNulls, requiredFields)
  }
}
@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportCommon {
  def toSymbol(field: String) = Symbol("imported_field_" + field)
}

@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportVertexList extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               fields: Seq[String]) extends MagicOutput(instance) {
    val vertices = vertexSet
    val attrs = fields.map {
      f => f -> vertexAttribute[String](vertices, ImportCommon.toSymbol(f))
    }.toMap
  }
  def fromJson(j: JsValue) = ImportVertexList(TypedJson.read[RowInput](j \ "input"))
  // SI-9650
  def apply(input: RowInput) = new ImportVertexList(input)
}
@deprecated("Replaced by table-based importing.", "1.7.0")
class ImportVertexList(val input: RowInput) extends ImportCommon
    with TypedMetaGraphOp[NoInput, ImportVertexList.Output] with Serializable {
  override def equals(o: Any) =
    o.isInstanceOf[ImportVertexList] && o.asInstanceOf[ImportVertexList].input == input
  import ImportVertexList._
  override val isHeavy = true
  override val hasCustomSaving = true // Single-pass import.
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, input.fields)
  override def toJson = Json.obj("input" -> input.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val entities = o.attrs.values.map(_.entity)
    val entitiesByName = entities.map(e => e.name -> e).toMap
    val inOrder = input.fields.map(f => entitiesByName(ImportCommon.toSymbol(f)))

    val lines = numberedLines(rc, input)
    rc.ioContext.writeAttributes(inOrder, lines)
  }
}

@deprecated("Replaced by table-based importing.", "1.7.0")
trait ImportEdges extends ImportCommon {
  val src: String
  val dst: String
  mustHaveField(src)
  mustHaveField(dst)

  def putEdgeAttributes(columns: Columns,
                        oattr: Map[String, EntityContainer[Attribute[String]]],
                        output: OutputBuilder): Unit = {
    for ((field, rdd) <- columns.singleColumns) {
      output(oattr(field), rdd)
    }
  }

  def edgeSrcDst(columns: Columns) = columns.columnPair(src, dst)
}

@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportEdgeList extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance,
               fields: Seq[String])
      extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val attrs = fields.map {
      f => f -> edgeAttribute[String](edges, ImportCommon.toSymbol(f))
    }.toMap
    val stringId = vertexAttribute[String](vertices)
  }
  def fromJson(j: JsValue) =
    ImportEdgeList(TypedJson.read[RowInput](j \ "input"), (j \ "src").as[String], (j \ "dst").as[String])
  // SI-9650
  def apply(input: RowInput, src: String, dst: String) = new ImportEdgeList(input, src, dst)
}
@deprecated("Replaced by table-based importing.", "1.7.0")
class ImportEdgeList(val input: RowInput, val src: String, val dst: String)
    extends ImportEdges
    with TypedMetaGraphOp[NoInput, ImportEdgeList.Output] with Serializable {
  override def equals(o: Any) = {
    o.isInstanceOf[ImportEdgeList] && {
      val other = o.asInstanceOf[ImportEdgeList]
      other.input == input && other.src == src && other.dst == dst
    }
  }
  import ImportEdgeList._
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, input.fields)
  override def toJson = Json.obj("input" -> input.toTypedJson, "src" -> src, "dst" -> dst)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val runtimeContext = rc
    val columns = readColumns(rc, input, Set(src, dst))
    val edgePartitioner = columns(src).partitioner.get
    putEdgeAttributes(columns, o.attrs, output)
    val names = columns.columnPair(src, dst).values.flatMap(sd => Iterator(sd._1, sd._2))
      .distinct
      .cache()
    val vertexPartitioner = rc.partitionerForNRows(names.count())
    val maxPartitioner = RDDUtils.maxPartitioner(edgePartitioner, vertexPartitioner)
    val idToName = names.randomNumbered(vertexPartitioner)
    val nameToId = idToName
      .map(_.swap)
      // This is going to be used in HybridRDD, so use the larger partitioner.
      .sortUnique(maxPartitioner)
    val edgesBySrc = edgeSrcDst(columns).map {
      case (edgeId, (src, dst)) => src -> (edgeId, dst)
    }
    val srcResolvedByDst = HybridRDD(edgesBySrc, maxPartitioner, even = true)
      .lookupAndRepartition(nameToId)
      .map { case (src, ((edgeId, dst), sid)) => dst -> (edgeId, sid) }

    val edges = HybridRDD(srcResolvedByDst, maxPartitioner, even = true)
      .lookup(nameToId)
      .map { case (dst, ((edgeId, sid), did)) => edgeId -> Edge(sid, did) }
      .sortUnique(edgePartitioner)

    output(o.edges, edges)
    output(o.vertices, idToName.mapValues(_ => ()))
    output(o.stringId, idToName)
  }
}

@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportEdgeListForExistingVertexSet extends OpFromJson {
  class Input extends MagicInputSignature {
    val sources = vertexSet
    val destinations = vertexSet
    val srcVidAttr = vertexAttribute[String](sources)
    val dstVidAttr = vertexAttribute[String](destinations)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input,
               fields: Seq[String])
      extends MagicOutput(instance) {
    val edges = edgeBundle(inputs.sources.entity, inputs.destinations.entity)
    val attrs = fields.map {
      f => f -> edgeAttribute[String](edges, ImportCommon.toSymbol(f))
    }.toMap
  }
  def fromJson(j: JsValue) =
    ImportEdgeListForExistingVertexSet(TypedJson.read[RowInput](j \ "input"), (j \ "src").as[String], (j \ "dst").as[String])
  // SI-9650
  def apply(input: RowInput, src: String, dst: String) =
    new ImportEdgeListForExistingVertexSet(input, src, dst)
}
@deprecated("Replaced by table-based importing.", "1.7.0")
class ImportEdgeListForExistingVertexSet(val input: RowInput, val src: String, val dst: String)
    extends ImportEdges
    with TypedMetaGraphOp[ImportEdgeListForExistingVertexSet.Input, ImportEdgeListForExistingVertexSet.Output]
    with Serializable {
  override def equals(o: Any) = {
    o.isInstanceOf[ImportEdgeListForExistingVertexSet] && {
      val other = o.asInstanceOf[ImportEdgeListForExistingVertexSet]
      other.input == input && other.src == src && other.dst == dst
    }
  }
  import ImportEdgeListForExistingVertexSet._
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs, input.fields)
  override def toJson = Json.obj("input" -> input.toTypedJson, "src" -> src, "dst" -> dst)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val runtimeContext = rc
    val columns = readColumns(rc, input, Set(src, dst))
    putEdgeAttributes(columns, o.attrs, output)

    val edges = ImportEdgesForExistingVertices.resolveEdges(
      edgeSrcDst(columns),
      inputs.srcVidAttr.data,
      inputs.dstVidAttr.data)

    output(o.edges, edges)
  }
}

@deprecated("Replaced by table-based importing.", "1.7.0")
object ImportAttributesForExistingVertexSet extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val idAttr = vertexAttribute[String](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input,
               fields: Set[String])
      extends MagicOutput(instance) {
    val attrs = fields.map {
      f => f -> vertexAttribute[String](inputs.vs.entity, ImportCommon.toSymbol(f))
    }.toMap
  }
  def fromJson(j: JsValue) =
    ImportAttributesForExistingVertexSet(TypedJson.read[RowInput](j \ "input"), (j \ "idField").as[String])
  // SI-9650
  def apply(input: RowInput, idField: String) =
    new ImportAttributesForExistingVertexSet(input, idField)
}
@deprecated("Replaced by table-based importing.", "1.7.0")
class ImportAttributesForExistingVertexSet(val input: RowInput, val idField: String)
    extends ImportCommon
    with TypedMetaGraphOp[ImportAttributesForExistingVertexSet.Input, ImportAttributesForExistingVertexSet.Output]
    with Serializable {
  override def equals(o: Any) = {
    o.isInstanceOf[ImportAttributesForExistingVertexSet] && {
      val other = o.asInstanceOf[ImportAttributesForExistingVertexSet]
      other.input == input && other.idField == idField
    }
  }
  import ImportAttributesForExistingVertexSet._

  mustHaveField(idField)

  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs, input.fields.toSet - idField)
  override def toJson = Json.obj("input" -> input.toTypedJson, "idField" -> idField)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val partitioner = inputs.vs.rdd.partitioner.get
    val lines = input.lines(rc).values
    val idFieldIdx = input.fields.indexOf(idField)
    val externalIdToInternalId =
      inputs.idAttr.rdd
        .map(_.swap)
        .assertUniqueKeys(partitioner)
    val linesByExternalId = lines
      .map(line => (line(idFieldIdx), line))
      .sortUnique(partitioner)
    val linesByInternalId =
      linesByExternalId.sortedJoin(externalIdToInternalId)
        .map { case (external, (line, internal)) => (internal, line) }
        .sortUnique(partitioner)
        .copyWithAncestorsCached
    for ((field, idx) <- input.fields.zipWithIndex) {
      if (idx != idFieldIdx) {
        output(o.attrs(field), linesByInternalId.mapValues(line => line(idx)))
      }
    }
  }
}
