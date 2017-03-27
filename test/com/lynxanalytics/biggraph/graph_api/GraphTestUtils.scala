package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.scalatest
import scala.util.Random
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.{ TestUtils, TestTempDir, TestSparkContext }

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util.{ PrefixRepository, HadoopFile, Timestamp }
import com.lynxanalytics.biggraph.registerStandardPrefixes
import com.lynxanalytics.biggraph.standardDataPrefix
import com.lynxanalytics.biggraph.spark_util.SQLHelper

import com.lynxanalytics.biggraph.spark_util.Implicits._

object GraphTestUtils {
  implicit class VertexSetOps[T <% VertexSetData](vs: T) {
    def toSeq(): Seq[ID] = {
      vs.rdd.keys.collect.toSeq.sorted
    }
  }

  implicit class EdgeBundleOps[T <% EdgeBundleData](eb: T) {
    def toPairSeq(): Seq[(ID, ID)] = {
      eb.rdd
        .collect
        .map { case (id, edge) => (edge.src -> edge.dst) }
        .toSeq
        .sorted
    }
    def toIdPairSeq(): Seq[(ID, (ID, ID))] = {
      eb.rdd
        .collect
        .map { case (id, edge) => (id, (edge.src, edge.dst)) }
        .toSeq
        .sorted
    }
  }
}

trait TestMetaGraphManager extends TestTempDir {
  def cleanMetaManagerDir = {
    val managerDir = tempDir(s"metaGraphManager.$Timestamp")
    managerDir.mkdir
    managerDir.toString
  }
  def cleanMetaManager: MetaGraphManager = MetaRepositoryManager(cleanMetaManagerDir)
}

trait TestDataManager extends TestTempDir with TestSparkContext {
  def cleanDataManager: DataManager = {
    val dataDir = cleanDataManagerDir()
    new DataManager(sparkSession, dataDir)
  }
}

// A TestDataManager that has an ephemeral path, too.
trait TestDataManagerEphemeral extends TestTempDir with TestSparkContext {
  // Override this if you want to do some preparation to the data directories before
  // the data manager is created.
  def prepareDataRepos(permanent: HadoopFile, ephemeral: HadoopFile): Unit = {
  }
  def cleanDataManagerEphemeral: DataManager = {
    val permanentDir = cleanDataManagerDir()
    val ephemeralDir = cleanDataManagerDir()
    prepareDataRepos(permanentDir, ephemeralDir)
    new DataManager(sparkSession, permanentDir, Some(ephemeralDir))
  }
}

trait TestGraphOp extends TestMetaGraphManager with TestDataManager with BigGraphEnvironment
    with scalatest.Suite with scalatest.BeforeAndAfter {
  PrefixRepository.dropResolutions()
  after {
    dataManager.waitAllFutures()
  }
  implicit val metaGraphManager = cleanMetaManager
  implicit val dataManager = cleanDataManager
  PrefixRepository.registerPrefix(standardDataPrefix, dataManager.repositoryPath.symbolicName)
  registerStandardPrefixes()
}

trait TestGraphOpEphemeral extends TestMetaGraphManager with TestDataManagerEphemeral {
  PrefixRepository.dropResolutions()
  implicit val metaGraphManager = cleanMetaManager
  implicit val dataManager = cleanDataManagerEphemeral
  PrefixRepository.registerPrefix(standardDataPrefix, dataManager.writablePath.symbolicName)
  registerStandardPrefixes()
}

case class TestGraph(vertices: VertexSet, edges: EdgeBundle, attrs: Map[String, Attribute[_]]) {
  def attr[T: TypeTag](name: String) = attrs(name).runtimeSafeCast[T]
}
object TestGraph {
  import Scripting._
  def loadCSV(file: String)(implicit mm: MetaGraphManager, dm: DataManager): SQLHelper.DataFrameOutput = {
    val df = dm.newSQLContext().read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(file)
    val t = ImportDataFrame.run(df)
    val op = TableToAttributes()
    op(op.t, t).result
  }
  // Loads a graph from vertices.csv and edges.csv.
  def fromCSV(directory: String)(implicit mm: MetaGraphManager, dm: DataManager): TestGraph = {
    val vertexCSV = loadCSV(directory + "/vertices.csv")
    val edgeCSV = loadCSV(directory + "/edges.csv")
    val edges = {
      val op = new ImportEdgeListForExistingVertexSetFromTable()
      op(
        op.srcVidColumn, edgeCSV.columns("src").runtimeSafeCast[String])(
          op.dstVidColumn, edgeCSV.columns("dst").runtimeSafeCast[String])(
            op.srcVidAttr, vertexCSV.columns("id").runtimeSafeCast[String])(
              op.dstVidAttr, vertexCSV.columns("id").runtimeSafeCast[String]).result.edges
    }
    TestGraph(vertexCSV.ids, edges, vertexCSV.columns.map {
      case (name, attr) => name -> attr.entity
    }.toMap)
  }
}

object SmallTestGraph extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vs, es) = graph
  }
  def fromJson(j: JsValue) = {
    SmallTestGraph(
      (j \ "edgeLists").as[Map[String, Array[Int]]].map { case (k, v) => k.toInt -> v.toSeq },
      (j \ "numPartitions").as[Int])
  }
}
case class SmallTestGraph(edgeLists: Map[Int, Seq[Int]], numPartitions: Int = 1)
    extends TypedMetaGraphOp[NoInput, SmallTestGraph.Output] {
  import SmallTestGraph._
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = {
    import play.api.libs.json
    Json.obj(
      "edgeLists" -> json.JsObject(edgeLists.toSeq.map { case (k, v) => k.toString -> json.JsArray(v.map(json.JsNumber(_))) }),
      "numPartitions" -> numPartitions)
  }

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    val p = new spark.HashPartitioner(numPartitions)
    output(
      o.vs,
      sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ())))
        .sortUnique(p))

    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    output(
      o.es,
      sc.parallelize(nodePairs.zipWithIndex.map {
        case ((a, b), i) => i.toLong -> Edge(a, b)
      })
        .sortUnique(p))
  }
}

object AddEdgeBundle extends OpFromJson {
  class Input extends MagicInputSignature {
    val vsA = vertexSet
    val vsB = vertexSet
  }
  class Output(
      implicit instance: MetaGraphOperationInstance,
      inputs: Input,
      properties: EdgeBundleProperties) extends MagicOutput(instance) {
    val esAB = edgeBundle(inputs.vsA.entity, inputs.vsB.entity, properties = properties)
  }
  def fromJson(j: JsValue) = AddEdgeBundle((j \ "edgeList").as[Seq[Seq[Int]]].map(ab => ab(0) -> ab(1)))
  def getFunctionProperties(edgeList: Seq[(Int, Int)]): EdgeBundleProperties = {
    val srcSet = edgeList.map(_._1).toSet
    val dstSet = edgeList.map(_._2).toSet
    EdgeBundleProperties(
      isFunction = (srcSet.size == edgeList.size),
      isReversedFunction = (dstSet.size == edgeList.size))
  }
}
case class AddEdgeBundle(edgeList: Seq[(Int, Int)])
    extends TypedMetaGraphOp[AddEdgeBundle.Input, AddEdgeBundle.Output] {
  import AddEdgeBundle._
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs, AddEdgeBundle.getFunctionProperties(edgeList))
  override def toJson = {
    Json.obj(
      "edgeList" -> edgeList.map { case (a, b) => Seq(a, b) })
  }

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    val es = sc.parallelize(
      edgeList.map {
        case (a, b) => Edge(a.toLong, b.toLong)
      }).randomNumbered(rc.onePartitionPartitioner)
    output(o.esAB, es)
  }
}

object SegmentedTestGraph extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val segments = vertexSet
    val belongsTo = edgeBundle(vs, segments)
  }
  def fromJson(j: JsValue) =
    SegmentedTestGraph((j \ "edgeLists").as[Seq[Seq[Int]]].map(s => s.tail -> s.head))
}
case class SegmentedTestGraph(edgeLists: Seq[(Seq[Int], Int)])
    extends TypedMetaGraphOp[NoInput, SegmentedTestGraph.Output] {
  import SegmentedTestGraph._
  @transient override lazy val inputs = new NoInput
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson =
    Json.obj("edgeLists" -> edgeLists.map { case (s, d) => d +: s })

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    val (srcs, dsts) = edgeLists.unzip
    val vs = sc.parallelize(
      srcs.flatten.map(_.toLong -> (())))
      .sortUnique(rc.onePartitionPartitioner)
    val segments = sc.parallelize(
      dsts.map(_.toLong -> (())))
      .sortUnique(rc.onePartitionPartitioner)
    val es = sc.parallelize(
      edgeLists.flatMap {
        case (s, i) => s.map(j => Edge(j.toLong, i.toLong))
      }).randomNumbered(rc.onePartitionPartitioner)
    output(o.vs, vs)
    output(o.segments, segments)
    output(o.belongsTo, es)
  }
}

object AddWeightedEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val es = edgeBundle(inputs.src.entity, inputs.dst.entity)
    val weight = edgeAttribute[Double](es)
  }
  def fromJson(j: JsValue) =
    AddWeightedEdges((j \ "edges").as[Seq[Seq[ID]]].map(ab => ab(0) -> ab(1)), (j \ "weight").as[Double])
}
case class AddWeightedEdges(edges: Seq[(ID, ID)], weight: Double)
    extends TypedMetaGraphOp[AddWeightedEdges.Input, AddWeightedEdges.Output] {
  import AddWeightedEdges._
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj(
    "edges" -> edges.map { case (a, b) => Seq(a, b) },
    "weight" -> weight)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val es = rc.sparkContext.parallelize(edges.map {
      case (a, b) => Edge(a, b)
    }).randomNumbered(rc.onePartitionPartitioner)
    output(o.es, es)
    output(o.weight, es.mapValues(_ => weight))
  }
}

object AddVertexAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output[T: TypeTag](instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    implicit val inst = instance
    val attr = vertexAttribute[T](inputs.vs.entity)
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    val st = SerializableType.fromJson(j \ "type")
    fromJsonTyped(j)(st)
  }
  def fromJsonTyped[T](j: JsValue)(implicit st: SerializableType[T]) = {
    import st.format
    AddVertexAttribute((j \ "values").as[Map[String, T]].map { case (k, v) => k.toInt -> v })
  }

  def run[T: TypeTag](
    vs: VertexSet, values: Map[Int, T])(implicit m: MetaGraphManager): Attribute[T] = {
    import Scripting._
    val op = AddVertexAttribute(values)(SerializableType[T])
    op(op.vs, vs).result.attr
  }
}
case class AddVertexAttribute[T](values: Map[Int, T])(implicit st: SerializableType[T])
    extends TypedMetaGraphOp[AddVertexAttribute.Input, AddVertexAttribute.Output[T]] {
  import AddVertexAttribute._

  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T](instance, inputs)(st.typeTag)
  override def toJson = {
    import st.format
    Json.obj(
      "values" -> values.map { case (k, v) => k.toString -> v },
      "type" -> st.toJson)
  }

  def execute(inputDatas: DataSet, o: Output[T], output: OutputBuilder, rc: RuntimeContext) = {
    import st.classTag
    implicit val id = inputDatas
    val sc = rc.sparkContext
    val idMap = values.toSeq.map { case (k, v) => k.toLong -> v }
    val partitioner = inputs.vs.rdd.partitioner.get
    output(o.attr, sc.parallelize(idMap).sortUnique(partitioner))
  }
}
