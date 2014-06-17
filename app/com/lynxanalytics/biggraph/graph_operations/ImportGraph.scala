package com.lynxanalytics.biggraph.graph_operations

import scala.util.{ Failure, Success, Try }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.RDDUtils
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

  // Splits a line of CSV, respecting double quotes.
  private[graph_operations] def split(line: String, delimiter: String): Seq[String] = {
    require(delimiter.length == 1)
    val delim = delimiter(0)
    val quote = '"'
    var i = 0
    val l = line.length
    val results = collection.mutable.Buffer[String]()
    while (i < l) {
      var start = i
      var end = l
      if (line(i) == quote) {
        // Quoted field.
        start += 1
        i += 1
        while (i < l && end == l) {
          if (line(i) == quote) {
            // It is a closing quote if the line ends or a delimiter follows.
            if (i == l - 1) {
              end = i
              i += 1
            } else if (line(i + 1) == delim) {
              end = i
              i += 1
            }
          }
          i += 1
        }
      } else {
        // Unquoted field.
        while (i < l && end == l) {
          // Break at next delimiter.
          if (line(i) == delim) {
            end = i
          }
          i += 1
        }
      }
      results += line.slice(start, end)
    }
    return results
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
        log.info(s"JS expression ($expression) returned $result instead of a Boolean")
        false // Treat errors as "false".
      case Failure(e) =>
        log.info(s"Could not evaluate JS: $expression", e)
        false // Treat errors as "false".
    }
  }
}
object Javascript {
  val engine = new javax.script.ScriptEngineManager().getEngineByName("JavaScript")
}

case class CSV(file: Filename,
               delimiter: String,
               header: String,
               filter: Javascript = Javascript("")) {
  val fields = ImportUtil.split(header, delimiter)

  def readAs[T](sc: SparkContext, extract: (Long, Seq[String], Int) => (Long, T)): Map[String, RDD[(Long, T)]] = {
    val lines = file.loadTextFile(sc)
    val filtered = lines
      .filter(_ != header)
      .map(ImportUtil.split(_, delimiter))
      .filter(jsFilter(_))
    val numbered = RDDUtils.fastNumbered(filtered)
    return fields.zipWithIndex.map {
      case (field, idx) => field -> numbered.map { case (id, line) => extract(id, line, idx) }
    }.toMap
  }

  def read(sc: SparkContext) = readAs(sc, {
    (id, line, idx) => id -> line(idx)
  })

  def jsFilter(line: Seq[String]): Boolean = {
    if (line.length != fields.length) {
      log.info(s"Input line cannot be parsed: $line")
      return false
    }
    return filter.isTrue(fields.zip(line).toMap)
  }
}

case class ImportVertexList(csv: CSV) extends MetaGraphOperation {
  assert(!csv.fields.contains("vertices"),
    "'vertices' is a reserved name and cannot be the name of a column.")

  def signature = {
    val s = newSignature.outputVertexSet('vertices)
    for (field <- csv.fields) {
      s.outputVertexAttribute[String](Symbol(field), 'vertices)
    }
    s
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    for ((field, rdd) <- read(rc.sparkContext)) {
      outputs.putVertexAttribute(Symbol(field), rdd)
      outputs.putVertexSet('vertices, rdd.keys.map((_, Unit))) // Doesn't matter which one.
    }
  }

  def read(sc: SparkContext): Map[String, RDD[(Long, String)]] = csv.read(sc)

  // A subclass of ImportVertexList, which uses a numeric field for the vertex ID.
  def withNumericId(field: String): ImportVertexList = new ImportVertexList(csv) {
    val idIdx = csv.fields.indexOf(field)
    override def read(sc: SparkContext) = csv.readAs(sc, {
      (id, line, idx) => line(idIdx).toLong -> line(idx)
    })
  }
}

case class ImportEdgeList(csv: CSV, srcId: String, dstId: String) extends MetaGraphOperation {
  assert(!csv.fields.contains("vertices"),
    "'vertices' is a reserved name and cannot be the name of a column.")
  assert(!csv.fields.contains("edges"),
    "'edges' is a reserved name and cannot be the name of a column.")
  assert(csv.fields.contains(srcId), s"No such field: $srcId in ${csv.fields}")
  assert(csv.fields.contains(dstId), s"No such field: $dstId in ${csv.fields}")

  def signature = {
    val s = newSignature.outputGraph('vertices, 'edges)
    addEdgeAttributes(s)
    s
  }

  def addEdgeAttributes(s: MetaGraphOperationSignature) = {
    for (field <- csv.fields) {
      s.outputEdgeAttribute[String](Symbol(field), 'edges)
    }
  }

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val sc = rc.sparkContext
    for ((field, rdd) <- csv.read(sc)) {
      outputs.putEdgeAttribute(Symbol(field), rdd)
    }
    outputs.putVertexSet('vertices, vertices(sc).map((_, Unit)))
    outputs.putEdgeBundle('edges, edges(sc))
  }

  def edgeSrcDst(sc: SparkContext): RDD[(Long, (String, String))] =
    csv.read(sc)(srcId).join(csv.read(sc)(dstId))

  def idToName(sc: SparkContext): RDD[(Long, String)] = {
    val vertices = edgeSrcDst(sc).flatMap {
      case (edge, (src, dst)) => Seq(src, dst)
    }
    return RDDUtils.fastNumbered(vertices.distinct)
  }

  def vertices(sc: SparkContext) = idToName(sc).values.map(_.toLong)

  def edges(sc: SparkContext) = edgeSrcDst(sc).mapValues {
    case (src, dst) => Edge(src.toLong, dst.toLong)
  }

  def read(sc: SparkContext): Map[String, RDD[(Long, String)]] = csv.read(sc)

  // A subclass of ImportEdgeList, which identifies vertices by strings.
  def withStringId(vertexAttr: String): ImportEdgeList = new ImportEdgeList(csv, srcId, dstId) {
    assert(!csv.fields.contains(vertexAttr), "Name collision on: $vertexAttr")
    assert(vertexAttr != "vertices",
      "'vertices' is a reserved name and cannot be the name of a column.")
    assert(vertexAttr != "edges",
      "'edges' is a reserved name and cannot be the name of a column.")

    override def signature =
      super.signature.outputVertexAttribute[String](Symbol(vertexAttr), 'vertices)

    override def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
      super.execute(inputs, outputs, rc)
      outputs.putVertexAttribute(Symbol(vertexAttr), idToName(rc.sparkContext))
    }

    override def vertices(sc: SparkContext): RDD[Long] = idToName(sc).keys

    override def edges(sc: SparkContext): EdgeBundleRDD = {
      val nameToId = idToName(sc).map { case (id, name) => (name, id) }
      val bySrc = edgeSrcDst(sc).map {
        case (edge, (src, dst)) => src -> (edge, dst)
      }
      val byDst = bySrc.join(nameToId).map {
        case (src, ((edge, dst), sid)) => dst -> (edge, sid)
      }
      return byDst.join(nameToId).map {
        case (dst, ((edge, sid), did)) => edge -> Edge(sid, did)
      }
    }

    override def forVertexSet = ??? // Cannot be combined.
  }

  // A subclass of ImportEdgeList, which adds the edge bundle to an existing vertex set.
  def forVertexSet: ImportEdgeList = new ImportEdgeList(csv, srcId, dstId) {
    assert(!csv.fields.contains("sources"),
      "'sources' is a reserved name and cannot be the name of a column.")
    assert(!csv.fields.contains("destinations"),
      "'destinations' is a reserved name and cannot be the name of a column.")

    override def signature = {
      val s = newSignature
        .inputVertexSet('sources)
        .inputVertexSet('destinations)
        .outputEdgeBundle('edges, 'sources -> 'destinations)
      addEdgeAttributes(s)
      s
    }

    override def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
      val sc = rc.sparkContext
      for ((field, rdd) <- csv.read(sc)) {
        outputs.putEdgeAttribute(Symbol(field), rdd)
      }
      outputs.putEdgeBundle('edges, edges(sc))
    }

    override def withStringId(vertexAttr: String) = ??? // Cannot be combined.
  }
}
