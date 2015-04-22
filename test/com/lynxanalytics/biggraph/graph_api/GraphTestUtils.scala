package com.lynxanalytics.biggraph.graph_api

import java.io.File
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.util.Random

import com.lynxanalytics.biggraph.TestTempDir
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.BigGraphEnvironment

import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util.DataFile
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_util.{ SandboxedPath, DataFile }

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
    def toPairCounts(): Map[(ID, ID), Int] = {
      eb.rdd
        .collect
        .map { case (id, edge) => (edge.src -> edge.dst) }
        .groupBy(identity)
        .toMap
        .mapValues(_.size)
    }
  }
}

trait TestMetaGraphManager extends TestTempDir {
  def cleanMetaManagerDir = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("metaGraphManager." + dirName)
    managerDir.mkdir
    managerDir.toString
  }
  def cleanMetaManager: MetaGraphManager = MetaRepositoryManager(cleanMetaManagerDir)
}

trait TestDataManager extends TestTempDir with TestSparkContext {
  def cleanDataManager: DataManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    val sandboxRoot = SandboxedPath.getDummyRootName(managerDir.toString)
    new DataManager(sparkContext, DataFile(sandboxRoot))
  }
}

trait TestGraphOp extends TestMetaGraphManager with TestDataManager {
  implicit val metaGraphManager = cleanMetaManager
  implicit val dataManager = cleanDataManager
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
    val p =
      if (numPartitions == 1) rc.onePartitionPartitioner
      else new spark.HashPartitioner(numPartitions)
    output(
      o.vs,
      sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ())))
        .toSortedRDD(p))

    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    output(
      o.es,
      sc.parallelize(nodePairs.zipWithIndex.map {
        case ((a, b), i) => i.toLong -> Edge(a, b)
      })
        .toSortedRDD(p))
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
      srcs.flatten.map(_.toLong -> ()))
      .toSortedRDD(rc.onePartitionPartitioner)
    val segments = sc.parallelize(
      dsts.map(_.toLong -> ()))
      .toSortedRDD(rc.onePartitionPartitioner)
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
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[String](inputs.vs.entity)
  }
  def fromJson(j: JsValue) =
    AddVertexAttribute((j \ "values").as[Map[String, String]].map { case (k, v) => k.toInt -> v })
}
case class AddVertexAttribute(values: Map[Int, String])
    extends TypedMetaGraphOp[AddVertexAttribute.Input, AddVertexAttribute.Output] {
  import AddVertexAttribute._
  @transient override lazy val inputs = new Input
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson =
    Json.obj("values" -> values.map { case (k, v) => k.toString -> v })
  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    val idMap = values.toSeq.map { case (k, v) => k.toLong -> v }
    output(o.attr, sc.parallelize(idMap).toSortedRDD(rc.onePartitionPartitioner))
  }
}
