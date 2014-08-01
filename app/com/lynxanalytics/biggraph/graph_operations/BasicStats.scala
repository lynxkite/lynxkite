package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

object CountVertices {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val count = scalar[Long]
  }
}
case class CountVertices()
    extends TypedMetaGraphOp[CountVertices.Input, CountVertices.Output] {
  @transient override lazy val inputs = new CountVertices.Input()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new CountVertices.Output()(instance)

  def execute(inputDatas: DataSet,
              o: CountVertices.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.count, inputs.vertices.rdd.count)
  }
}

object CountEdges {
  class Input extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val edges = edgeBundle(srcVS, dstVS)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val count = scalar[Long]
  }
}
case class CountEdges()
    extends TypedMetaGraphOp[CountEdges.Input, CountEdges.Output] {
  import CountEdges._
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.count, inputs.edges.rdd.count)
  }
}

object ComputeMinMax {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val attribute = vertexAttribute[T](vertices)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.attribute.typeTag
    val min = scalar[T]
    val max = scalar[T]
  }
}
case class ComputeMinMax[T: Numeric]()
    extends TypedMetaGraphOp[ComputeMinMax.Input[T], ComputeMinMax.Output[T]] {
  @transient override lazy val inputs = new ComputeMinMax.Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new ComputeMinMax.Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: ComputeMinMax.Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attribute.data.typeTag
    implicit val ct = inputs.attribute.data.classTag
    val num = implicitly[Numeric[T]]
    val resOpt = inputs.attribute.rdd.values
      .aggregate(None: Option[(T, T)])(
        (minmax, next) => {
          val min = minmax.fold(next)(a => num.min(a._1, next))
          val max = minmax.fold(next)(a => num.max(a._2, next))
          Some(min, max)
        },
        (minmax1, minmax2) => {
          if (minmax1.isEmpty) minmax2
          else if (minmax2.isEmpty) minmax1
          else {
            val min = num.min(minmax1.get._1, minmax2.get._1)
            val max = num.max(minmax1.get._2, minmax2.get._2)
            Some(min, max)
          }
        })
    val res = resOpt.getOrElse(num.zero, num.zero)
    output(o.min, res._1)
    output(o.max, res._2)
  }
}

object ComputeTopValues {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val attribute = vertexAttribute[T](vertices)
  }
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: Input[T]) extends MagicOutput(instance) {
    implicit val tt = inputs.attribute.typeTag
    val topValues = scalar[Seq[(T, Int)]]
  }
  class PairOrdering[T] extends Ordering[(T, Int)] {
    def compare(a: (T, Int), b: (T, Int)) = {
      if (a._2 == b._2) a._1.hashCode compare b._1.hashCode
      else a._2 compare b._2
    }
  }
}
case class ComputeTopValues[T](numTopValues: Int)
    extends TypedMetaGraphOp[ComputeTopValues.Input[T], ComputeTopValues.Output[T]] {
  @transient override lazy val inputs = new ComputeTopValues.Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new ComputeTopValues.Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: ComputeTopValues.Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attribute.data.typeTag
    implicit val ct = inputs.attribute.data.classTag
    output(o.topValues,
      inputs.attribute.rdd
        .map { case (id, value) => (value, 1) }
        .reduceByKey(_ + _)
        .top(numTopValues)(new ComputeTopValues.PairOrdering[T])
        .toSeq)
  }
}
