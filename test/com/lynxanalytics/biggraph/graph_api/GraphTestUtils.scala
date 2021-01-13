package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.scalatest
import scala.util.Random
import scala.reflect.runtime.universe.TypeTag
import scala.language.implicitConversions
import org.apache.commons.io.FileUtils

import com.lynxanalytics.biggraph.{ TestUtils, TestTempDir, TestSparkContext }

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util.{ PrefixRepository, HadoopFile, Timestamp }
import com.lynxanalytics.biggraph.registerStandardPrefixes
import com.lynxanalytics.biggraph.standardDataPrefix
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment

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
  def computeAndGet(e: MetaGraphEntity)(
    implicit
    dm: DataManager, sd: SparkDomain): EntityData = {
    implicit val ec = dm.executionContext
    dm.await(dm.ensure(e, sd).map(_ => sd.getData(e)))
  }

  implicit def getVertexSetDataEC(e: EntityContainer[VertexSet])(
    implicit
    dm: DataManager, sd: SparkDomain): VertexSetData = {
    computeAndGet(e.entity).asInstanceOf[VertexSetData]
  }
  implicit def getEdgeBundleDataEC(e: EntityContainer[EdgeBundle])(
    implicit
    dm: DataManager, sd: SparkDomain): EdgeBundleData = {
    computeAndGet(e.entity).asInstanceOf[EdgeBundleData]
  }
  implicit def getAttributeDataEC[T](e: EntityContainer[Attribute[T]])(
    implicit
    dm: DataManager, sd: SparkDomain): AttributeData[T] = {
    computeAndGet(e.entity).asInstanceOf[AttributeData[T]]
  }
  implicit def getTableDataEC(e: EntityContainer[Table])(
    implicit
    dm: DataManager, sd: SparkDomain): TableData = {
    computeAndGet(e.entity).asInstanceOf[TableData]
  }

  implicit def getVertexSetData(entity: VertexSet)(
    implicit
    dm: DataManager, sd: SparkDomain): VertexSetData = {
    computeAndGet(entity).asInstanceOf[VertexSetData]
  }
  implicit def getEdgeBundleData(entity: EdgeBundle)(
    implicit
    dm: DataManager, sd: SparkDomain): EdgeBundleData = {
    computeAndGet(entity).asInstanceOf[EdgeBundleData]
  }
  implicit def getAttributeData[T](entity: Attribute[T])(
    implicit
    dm: DataManager, sd: SparkDomain): AttributeData[T] = {
    computeAndGet(entity).asInstanceOf[AttributeData[T]]
  }
  implicit def getTableData(entity: Table)(
    implicit
    dm: DataManager, sd: SparkDomain): TableData = {
    computeAndGet(entity).asInstanceOf[TableData]
  }

  import Scripting._
  def get(e: EdgeBundle)(implicit mm: MetaGraphManager, dm: DataManager): Map[ID, Edge] = {
    val res = {
      val op = EdgeBundleAsScalar()
      op(op.es, e).result
    }
    dm.get(res.sc)
  }
  def get(e: VertexSet)(implicit mm: MetaGraphManager, dm: DataManager): Set[ID] = {
    val res = {
      val op = VertexSetAsScalar()
      op(op.vs, e).result
    }
    dm.get(res.sc)
  }
  def get[T](e: Attribute[T])(implicit mm: MetaGraphManager, dm: DataManager): Map[ID, T] = {
    val res = {
      val op = AttributeAsScalar[T]()
      op(op.attr, e).result
    }
    dm.get(res.sc)
  }
  // For the sake of uniform syntax we also do it for scalars.
  def get[T](e: Scalar[T])(implicit dm: DataManager): T = dm.get(e)
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
  def cleanSparkDomain: SparkDomain = {
    val dataDir = cleanDataManagerDir()
    new SparkDomain(sparkSession, dataDir)
  }

  def cleanDataManager: DataManager = {
    val withSphynx = LoggedEnvironment.envOrElse("WITH_SPHYNX", "false").toBoolean
    if (withSphynx) {
      val dataDir = cleanDataManagerDir()
      val host = "localhost"
      val port = LoggedEnvironment.envOrNone("SPHYNX_PORT").get
      val certDir = LoggedEnvironment.envOrNone("SPHYNX_CERT_DIR").get
      val unorderedDataDir = LoggedEnvironment.envOrNone("UNORDERED_SPHYNX_DATA_DIR").get

      val dm = new DataManager(Seq(
        new OrderedSphynxDisk(host, port.toInt, certDir),
        new SphynxMemory(host, port.toInt, certDir),
        new UnorderedSphynxLocalDisk(host, port.toInt, certDir, unorderedDataDir),
        new ScalaDomain,
        new UnorderedSphynxSparkDisk(host, port.toInt, certDir, dataDir / "sphynx"),
        new SparkDomain(sparkSession, dataDir)))
      dm.domains.filter(_.isInstanceOf[SphynxDomain]).map(_.asInstanceOf[SphynxDomain].clear).foreach(
        _.awaitReady(concurrent.duration.Duration.Inf))
      dm
    } else {
      new DataManager(Seq(new ScalaDomain, cleanSparkDomain))
    }
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
    new DataManager(Seq(
      new ScalaDomain,
      new SparkDomain(sparkSession, permanentDir, Some(ephemeralDir))))
  }
}

trait TestGraphOp extends TestMetaGraphManager with TestDataManager with BigGraphEnvironment
  with scalatest.Suite with scalatest.BeforeAndAfter with scalatest.BeforeAndAfterAll {
  PrefixRepository.dropResolutions()
  after {
    dataManager.waitAllFutures()
  }
  override protected def afterAll {
    // We create new Sphynx domains for each test class. Each domain has its own channel
    // to the Sphynx server and after the tests finished, we ask the domains to shutdown
    // their channel. (Otherwise we get warnings about orphaned channels.)
    dataManager.domains.filter(_.isInstanceOf[SphynxDomain]).foreach(_.asInstanceOf[SphynxDomain].shutDownChannel)
  }
  implicit val metaGraphManager = cleanMetaManager
  implicit val dataManager = cleanDataManager
  implicit val sparkDomain = dataManager.domains.find(_.isInstanceOf[SparkDomain]).get.asInstanceOf[SparkDomain]
  PrefixRepository.registerPrefix(standardDataPrefix, sparkDomain.repositoryPath.symbolicName)
  registerStandardPrefixes()
}

trait TestGraphOpEphemeral extends TestMetaGraphManager with TestDataManagerEphemeral {
  PrefixRepository.dropResolutions()
  implicit val metaGraphManager = cleanMetaManager
  implicit val dataManager = cleanDataManagerEphemeral
  implicit val sparkDomain = dataManager.domains.find(_.isInstanceOf[SparkDomain]).get.asInstanceOf[SparkDomain]
  PrefixRepository.registerPrefix(standardDataPrefix, sparkDomain.writablePath.symbolicName)
  registerStandardPrefixes()
}

case class TestGraph(vertices: VertexSet, edges: EdgeBundle, attrs: Map[String, Attribute[_]]) {
  def attr[T: TypeTag](name: String) = attrs(name).runtimeSafeCast[T]
}
object TestGraph {
  import Scripting._
  def loadCSV(file: String)(
    implicit
    mm: MetaGraphManager, sd: SparkDomain): TableToAttributes.Output = {
    val df = sd.newSQLContext().read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(file)
    val t = ImportDataFrame.run(df)
    val op = TableToAttributes()
    op(op.t, t).result
  }
  // Loads a graph from vertices.csv and edges.csv.
  def fromCSV(directory: String)(implicit mm: MetaGraphManager, sd: SparkDomain): TestGraph = {
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
  extends SparkOperation[NoInput, SmallTestGraph.Output] {
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
      implicit
      instance: MetaGraphOperationInstance,
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
  extends SparkOperation[AddEdgeBundle.Input, AddEdgeBundle.Output] {
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
  extends SparkOperation[NoInput, SegmentedTestGraph.Output] {
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
  extends SparkOperation[AddWeightedEdges.Input, AddWeightedEdges.Output] {
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
  extends SparkOperation[AddVertexAttribute.Input, AddVertexAttribute.Output[T]] {
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

object VertexSetAsScalar extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sc = scalar[Set[ID]]
  }
  def fromJson(j: JsValue) = VertexSetAsScalar()
}
case class VertexSetAsScalar()
  extends ScalaOperation[VertexSetAsScalar.Input, VertexSetAsScalar.Output] {
  import VertexSetAsScalar._
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj()
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]) = {
    output('sc) = input('vs)
  }
}

object EdgeBundleAsScalar extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val es = edgeBundle(src, dst)
  }
  class Output(
      implicit
      instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sc = scalar[Map[ID, Edge]]
  }
  def fromJson(j: JsValue) = EdgeBundleAsScalar()
}
case class EdgeBundleAsScalar() extends ScalaOperation[EdgeBundleAsScalar.Input, EdgeBundleAsScalar.Output] {
  import EdgeBundleAsScalar._
  @transient override lazy val inputs = new EdgeBundleAsScalar.Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj()
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]) = {
    output('sc) = input('es)
  }
}

object AttributeAsScalar extends OpFromJson {
  class Output[T: TypeTag](
      implicit
      instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val sc = scalar[Map[ID, T]]
  }
  def fromJson(j: JsValue) = AttributeAsScalar()
}
case class AttributeAsScalar[T]()
  extends ScalaOperation[VertexAttributeInput[T], AttributeAsScalar.Output[T]] {
  import AttributeAsScalar._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new Output[T]()(inputs.attr.entity.typeTag, instance)
  }
  override def toJson = Json.obj()
  def execute(input: Map[Symbol, Any], output: collection.mutable.Map[Symbol, Any]) = {
    output('sc) = input('attr)
  }
}
