package com.lynxanalytics.biggraph.graph_api

import java.io.File
import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.util.Random

import com.lynxanalytics.biggraph.TestTempDir
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.BigGraphEnvironment

import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.Implicits._

object GraphTestUtils {
  implicit class VertexSetOps[T <% VertexSetData](vs: T) {
    def toSet(): Set[ID] = {
      vs.rdd.keys.collect.toSet
    }
  }

  implicit class EdgeBundleOps[T <% EdgeBundleData](eb: T) {
    def toMap(): Map[ID, ID] = {
      eb.rdd
        .collect
        .map { case (id, edge) => (edge.src -> edge.dst) }
        .toMap
    }
  }
}

trait TestMetaGraphManager extends TestTempDir {
  def cleanMetaManager: MetaGraphManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("metaGraphManager." + dirName)
    managerDir.mkdir
    new MetaGraphManager(managerDir.toString)
  }
}

trait TestDataManager extends TestTempDir with TestSparkContext {
  def cleanDataManager: DataManager = {
    val dirName = getClass.getName + "." + Random.alphanumeric.take(5).mkString
    val managerDir = tempDir("dataManager." + dirName)
    managerDir.mkdir
    new DataManager(sparkContext, Filename(managerDir.toString))
  }
}

class GraphOperationTestHelper(val metaManager: MetaGraphManager,
                               val dataManager: DataManager) {
  def apply(operation: TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider],
            inputs: MetaDataSet = MetaDataSet()): MetaDataSet = {
    metaManager.apply(operation, inputs).outputs
  }

  def apply(operation: TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider],
            all: Map[Symbol, MetaGraphEntity]): MetaDataSet = {
    apply(operation, MetaDataSet(all))
  }

  def apply(operation: TypedMetaGraphOp[_ <: InputSignatureProvider, _ <: MetaDataSetProvider],
            all: (Symbol, MetaGraphEntity)*): MetaDataSet = {
    metaManager.apply(operation, all: _*).outputs
  }

  def smallGraph(edgeLists: Map[Int, Seq[Int]]): (VertexSet, EdgeBundle) = {
    val outs = apply(SmallTestGraph(edgeLists))
    (outs.vertexSets('vs), outs.edgeBundles('es))
  }

  def rdd(vertexSet: VertexSet): VertexSetRDD = dataManager.get(vertexSet).rdd
  def localData(vertexSet: VertexSet): Set[Long] = rdd(vertexSet).keys.collect.toSet

  def rdd(edgeBundle: EdgeBundle): EdgeBundleRDD = dataManager.get(edgeBundle).rdd
  def localData(edgeBundle: EdgeBundle): Set[(Long, Long)] = {
    rdd(edgeBundle)
      .collect
      .map { case (id, edge) => (edge.src, edge.dst) }
      .toSet
  }

  def rdd[T](vertexAttribute: VertexAttribute[T]): AttributeRDD[T] =
    dataManager.get(vertexAttribute).rdd
  def localData[T: reflect.runtime.universe.TypeTag](vertexAttribute: VertexAttribute[_]): Map[Long, T] =
    localData(vertexAttribute.runtimeSafeCast[T])
  def localData[T](vertexAttribute: VertexAttribute[T]): Map[Long, T] =
    rdd(vertexAttribute).collect.toMap

  def rdd[T](edgeAttribute: EdgeAttribute[T]): AttributeRDD[T] =
    dataManager.get(edgeAttribute).rdd
  def localData[T: reflect.runtime.universe.TypeTag](edgeAttribute: EdgeAttribute[_]): Map[Long, T] =
    localData(edgeAttribute.runtimeSafeCast[T])
  def localData[T](edgeAttribute: EdgeAttribute[T]): Map[(Long, Long), T] = {
    val edgesRDD = rdd(edgeAttribute.edgeBundle)
    val attrRDD = rdd(edgeAttribute)
    edgesRDD.join(attrRDD).map {
      case (id, (edge, value)) =>
        (edge.src, edge.dst) -> value
    }.collect.toMap
  }

  def localData[T](scalar: Scalar[T]): T = dataManager.get(scalar).value
}

object HelperSingletonProvider extends TestMetaGraphManager with TestDataManager {
  lazy val helper = new GraphOperationTestHelper(cleanMetaManager, cleanDataManager)
}

trait TestGraphOperation extends TestMetaGraphManager with TestDataManager {
  val helper = HelperSingletonProvider.helper
  def cleanHelper = new GraphOperationTestHelper(cleanMetaManager, cleanDataManager)
}

trait TestGraphOp extends TestMetaGraphManager with TestDataManager {
  implicit val metaManager = cleanMetaManager
  implicit val dataManager = cleanDataManager
}

object SmallTestGraph {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val (vs, es) = graph
  }
}
case class SmallTestGraph(edgeLists: Map[Int, Seq[Int]])
    extends TypedMetaGraphOp[NoInput, SmallTestGraph.Output] {
  import SmallTestGraph._
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    output(
      o.vs,
      sc.parallelize(edgeLists.keys.toList.map(i => (i.toLong, ())))
        .partitionBy(rc.onePartitionPartitioner))

    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    output(
      o.es,
      sc.parallelize(nodePairs.zipWithIndex.map {
        case ((a, b), i) => i.toLong -> Edge(a, b)
      })
        .partitionBy(rc.onePartitionPartitioner))
  }
}

object SegmentedTestGraph {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val segments = vertexSet
    val belongsTo = edgeBundle(vs, segments)
  }
}
case class SegmentedTestGraph(edgeLists: Seq[(Seq[Int], Int)])
    extends TypedMetaGraphOp[NoInput, SegmentedTestGraph.Output] {
  import SegmentedTestGraph._
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val sc = rc.sparkContext
    val (srcs, dsts) = edgeLists.unzip
    val vs: VertexSetRDD = sc.parallelize(
      srcs.flatten.map(_.toLong -> ()))
      .partitionBy(rc.onePartitionPartitioner)
    val segments: VertexSetRDD = sc.parallelize(
      dsts.map(_.toLong -> ()))
      .partitionBy(rc.onePartitionPartitioner)
    val es: EdgeBundleRDD = sc.parallelize(
      edgeLists.flatMap {
        case (s, i) => s.map(j => Edge(j.toLong, i.toLong))
      }).fastNumbered(rc.onePartitionPartitioner)
    output(o.vs, vs)
    output(o.segments, segments)
    output(o.belongsTo, es)
  }
}

object AddWeightedEdges {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance, inputs: Input) extends MagicOutput(instance) {
    val es = edgeBundle(inputs.src.entity, inputs.dst.entity)
    val weight = edgeAttribute[Double](es)
  }
}
case class AddWeightedEdges(edges: Seq[(ID, ID)], weight: Double)
    extends TypedMetaGraphOp[AddWeightedEdges.Input, AddWeightedEdges.Output] {
  import AddWeightedEdges._
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet, o: Output, output: OutputBuilder, rc: RuntimeContext) = {
    val es = rc.sparkContext.parallelize(edges.map {
      case (a, b) => Edge(a, b)
    }).fastNumbered(rc.onePartitionPartitioner)
    output(o.es, es)
    output(o.weight, es.mapValues(_ => weight))
  }
}
