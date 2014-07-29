package com.lynxanalytics.biggraph.graph_operations

import scala.util.{ Failure, Success, Try }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext

// Functions for looking at CSV files. The frontend can use these when
// constructing the import operation.
object ImportUtil {
  def header(file: Filename): String =
    file.reader.readLine

  def fields(file: Filename, delimiter: String): Seq[String] =
    split(header(file), delimiter)

  private[graph_operations] def splitter(delimiter: String): String => Seq[String] = {
    val delim = java.util.regex.Pattern.quote(delimiter)
    def oneOf(options: String*) = options.mkString("|")
    def any(p: String) = capture(p) + "*"
    def capture(p: String) = "(" + p + ")"
    def oneField(p: String) = oneOf(p + delim, p + "$") // Delimiter or line end.
    val quote = "\""
    val nonQuote = "[^\"]"
    val doubleQuote = quote + quote
    val quotedString = capture(quote + any(oneOf(nonQuote, doubleQuote)) + quote)
    val anyString = capture(".*?")
    val r = oneOf(oneField(quotedString), oneField(anyString)).r
    val splitter = { line: String =>
      val matches = r.findAllMatchIn(line)
      // Find the top-level group that has matched in each field.
      val fields = matches.map(_.subgroups.find(_ != null).get).toList
      val l = fields.length
      // The last field may be a mistake. (Sorry, I couldn't write a better regex.)
      val fixed = if (l < 2 || fields(l - 2).endsWith(delimiter)) fields else fields.take(l - 1)
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

case class Javascript(expression: String) {
  def isEmpty = expression.isEmpty
  def nonEmpty = expression.nonEmpty

  def isTrue(mapping: (String, String)*): Boolean = isTrue(mapping.toMap)
  def isTrue(mapping: Map[String, String]): Boolean = {
    if (isEmpty) {
      return true
    }
    val bindings = Javascript.engine.createBindings
    for ((key, value) <- mapping) {
      bindings.put(key, value)
    }
    return Try(Javascript.engine.eval(expression, bindings)) match {
      case Success(result: java.lang.Boolean) =>
        result
      case Success(result) =>
        throw Javascript.Error(s"JS expression ($expression) returned $result instead of a Boolean")
      case Failure(e) =>
        throw Javascript.Error(s"Could not evaluate JS: $expression", e)
    }
  }
}
object Javascript {
  val engine = new javax.script.ScriptEngineManager().getEngineByName("JavaScript")
  case class Error(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}

case class CSV(file: Filename,
               delimiter: String,
               header: String,
               filter: Javascript = Javascript("")) {
  val fields = ImportUtil.split(header, delimiter)

  def lines(sc: SparkContext): RDD[Seq[String]] = {
    val lines = file.loadTextFile(sc)
    return lines
      .filter(_ != header)
      .map(ImportUtil.split(_, delimiter))
      .filter(jsFilter(_)).cache
  }

  def jsFilter(line: Seq[String]): Boolean = {
    if (line.length != fields.length) {
      log.info(s"Input line cannot be parsed: $line")
      return false
    }
    return filter.isTrue(fields.zip(line).toMap)
  }
}

abstract class ImportCommon {
  type Columns = Map[String, RDD[(ID, String)]]
  val csv: CSV

  protected def mustHaveField(field: String) = {
    assert(csv.fields.contains(field), s"No such field: $field in ${csv.fields}")
  }

  protected def readColumns(rc: RuntimeContext, csv: CSV): Columns = {
    val lines = csv.lines(rc.sparkContext)
    val numbered = lines.randomNumbered(rc.defaultPartitioner.numPartitions).cache
    return csv.fields.zipWithIndex.map {
      case (field, idx) => field -> numbered.mapValues(line => line(idx))
    }.toMap
  }

  protected def putEdgeAttributes(columns: Columns,
                                  oattr: Map[String, EdgeAttribute[String]],
                                  output: OutputBuilder): Unit = {
    for ((field, rdd) <- columns) {
      output(oattr(field), rdd)
    }
  }
}
object ImportCommon {
  def toSymbol(field: String) = Symbol("csv_" + field)
}

object ImportVertexList {
  class Output(implicit instance: MetaGraphOperationInstance,
               fields: Seq[String]) extends MagicOutput(instance) {
    val vertices = vertexSet
    val attrs = fields.map {
      f => f -> vertexAttribute[String](vertices, ImportCommon.toSymbol(f))
    }.toMap
  }
}
case class ImportVertexList(csv: CSV) extends ImportCommon
    with TypedMetaGraphOp[NoInput, ImportVertexList.Output] {
  import ImportVertexList._
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, csv.fields)
  override val isHeavy = true

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val columns = readColumns(rc, csv)
    for ((field, rdd) <- columns) {
      output(o.attrs(field), rdd)
    }
    output(o.vertices, columns.values.head.mapValues(_ => ()))
  }
}

object ImportEdgeList {
  class Output(implicit instance: MetaGraphOperationInstance,
               fields: Seq[String])
      extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val attrs = fields.map {
      f => f -> edgeAttribute[String](edges, ImportCommon.toSymbol(f))
    }.toMap
    val stringID = vertexAttribute[String](vertices)
  }
}
case class ImportEdgeList(csv: CSV, src: String, dst: String)
    extends ImportCommon with TypedMetaGraphOp[NoInput, ImportEdgeList.Output] {
  import ImportEdgeList._
  mustHaveField(src)
  mustHaveField(dst)
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, csv.fields)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val columns = readColumns(rc, csv)
    putEdgeAttributes(columns, o.attrs.mapValues(_.entity), output)
    val names = (columns(src).values ++ columns(dst).values).distinct
    val idToName = names.randomNumbered(rc.defaultPartitioner.numPartitions)
    val nameToId = idToName.map { case (id, name) => (name, id) }
    val edgeSrcDst = columns(src).join(columns(dst))
    val bySrc = edgeSrcDst.map {
      case (edge, (src, dst)) => src -> (edge, dst)
    }
    val byDst = bySrc.join(nameToId).map {
      case (src, ((edge, dst), sid)) => dst -> (edge, sid)
    }
    val edges = byDst.join(nameToId).map {
      case (dst, ((edge, sid), did)) => edge -> Edge(sid, did)
    }
    output(o.edges, edges.partitionBy(rc.defaultPartitioner))
    output(o.vertices, idToName.mapValues(_ => ()))
    output(o.stringID, idToName)
  }

  override val isHeavy = true
}

object ImportEdgeListForExistingVertexSet {
  class Input extends MagicInputSignature {
    val sources = vertexSet
    val destinations = vertexSet
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
}
case class ImportEdgeListForExistingVertexSet(csv: CSV, src: String, dst: String)
    extends ImportCommon
    with TypedMetaGraphOp[ImportEdgeListForExistingVertexSet.Input, ImportEdgeListForExistingVertexSet.Output] {
  import ImportEdgeListForExistingVertexSet._
  mustHaveField(src)
  mustHaveField(dst)
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs, csv.fields)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val columns = readColumns(rc, csv)
    putEdgeAttributes(columns, o.attrs.mapValues(_.entity), output)
    output(o.edges, columns(src).join(columns(dst)).mapValues {
      case (src, dst) => Edge(src.toLong, dst.toLong)
    })
  }

  override val isHeavy = true
}
