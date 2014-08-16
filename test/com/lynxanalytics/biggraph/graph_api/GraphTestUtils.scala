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
    def toPairSet(): Set[(ID, ID)] = {
      eb.rdd
        .collect
        .map { case (id, edge) => (edge.src -> edge.dst) }
        .toSet
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

trait TestGraphOp extends TestMetaGraphManager with TestDataManager {
  implicit val metaGraphManager = cleanMetaManager
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
        .toSortedRDD(rc.onePartitionPartitioner))

    val nodePairs = edgeLists.toSeq.flatMap {
      case (i, es) => es.map(e => i -> e)
    }
    output(
      o.es,
      sc.parallelize(nodePairs.zipWithIndex.map {
        case ((a, b), i) => i.toLong -> Edge(a, b)
      })
        .toSortedRDD(rc.onePartitionPartitioner))
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
    }).randomNumbered(rc.onePartitionPartitioner)
    output(o.es, es)
    output(o.weight, es.mapValues(_ => weight))
  }
}
