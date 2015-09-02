// Very simple operations for counting vertices, etc.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

// Convenient shorthands for counting.
object Count {
  import Scripting._

  def run(vertices: VertexSet)(implicit manager: MetaGraphManager): Scalar[Long] = {
    val op = CountVertices()
    op(op.vertices, vertices).result.count
  }

  def run(edges: EdgeBundle)(implicit manager: MetaGraphManager): Scalar[Long] =
    run(edges.idSet)
}

object CountVertices extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val count = scalar[Long]
  }
  def fromJson(j: JsValue) = CountVertices()
}
case class CountVertices()
    extends TypedMetaGraphOp[CountVertices.Input, CountVertices.Output] {
  override val isHeavy = true
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

// CountEdges is deprecated. Use Count.run(eb) instead.
object CountEdges extends OpFromJson {
  class Input extends MagicInputSignature {
    val srcVS = vertexSet
    val dstVS = vertexSet
    val edges = edgeBundle(srcVS, dstVS)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val count = scalar[Long]
  }
  def fromJson(j: JsValue) = CountEdges()
}
case class CountEdges()
    extends TypedMetaGraphOp[CountEdges.Input, CountEdges.Output] {
  import CountEdges._
  override val isHeavy = true
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

object CountAttributes extends OpFromJson {
  class Input[T] extends MagicInputSignature {
    val vertices = vertexSet
    val attribute = vertexAttribute[T](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val count = scalar[Long]
  }
  def fromJson(j: JsValue) = CountAttributes()
}
case class CountAttributes[T]()
    extends TypedMetaGraphOp[CountAttributes.Input[T], CountAttributes.Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new CountAttributes.Input[T]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new CountAttributes.Output()(instance)

  def execute(inputDatas: DataSet,
              o: CountAttributes.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.count, inputs.attribute.rdd.count)
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
    val min = scalar[Option[T]]
    val max = scalar[Option[T]]
    val minPositive = scalar[Option[T]]
  }
}
abstract class ComputeMinMax[T: Numeric]
    extends TypedMetaGraphOp[ComputeMinMax.Input[T], ComputeMinMax.Output[T]] {
  override val isHeavy = true
  @transient override lazy val inputs = new ComputeMinMax.Input[T]
  private lazy val num = implicitly[Numeric[T]]

  def outputMeta(instance: MetaGraphOperationInstance) =
    new ComputeMinMax.Output()(instance, inputs)

  private def smaller(a: Option[T], b: Option[T]): Option[T] = {
    (a, b) match {
      case (Some(a), Some(b)) => Some(num.min(a, b))
      case (a, b) => a.orElse(b)
    }
  }

  private def bigger(a: Option[T], b: Option[T]): Option[T] = {
    (a, b) match {
      case (Some(a), Some(b)) => Some(num.max(a, b))
      case (a, b) => a.orElse(b)
    }
  }

  def execute(inputDatas: DataSet,
              o: ComputeMinMax.Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val tt = inputs.attribute.data.typeTag
    implicit val ct = inputs.attribute.data.classTag
    import num.mkOrderingOps
    val (min, max, minpos) = inputs.attribute.rdd.values
      .aggregate((None: Option[T], None: Option[T], None: Option[T]))(
        {
          case ((min, max, minpos), next) =>
            (smaller(min, Some(next)),
              bigger(max, Some(next)),
              if (next > num.zero) smaller(minpos, Some(next)) else None)
        },
        {
          case ((min1, max1, minpos1), (min2, max2, minpos2)) =>
            (smaller(min1, min2),
              bigger(max1, max2),
              smaller(minpos1, minpos2))
        })
    output(o.min, min)
    output(o.max, max)
    output(o.minPositive, minpos)
  }
}

object ComputeMinMaxDouble extends OpFromJson {
  def fromJson(j: JsValue) = ComputeMinMaxDouble()
}
case class ComputeMinMaxDouble() extends ComputeMinMax[Double]

object ComputeTopValues extends OpFromJson {
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
  def fromJson(j: JsValue) =
    ComputeTopValues((j \ "numTopValues").as[Int], (j \ "sampleSize").as[Int])
}
case class ComputeTopValues[T](numTopValues: Int, sampleSize: Int = -1)
    extends TypedMetaGraphOp[ComputeTopValues.Input[T], ComputeTopValues.Output[T]] {
  override val isHeavy = true
  override def toJson =
    Json.obj("numTopValues" -> numTopValues, "sampleSize" -> sampleSize)
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
    val ordering = new ComputeTopValues.PairOrdering[T]
    val attribute = inputs.attribute.rdd
    val sampled =
      if (sampleSize > 0) attribute.coalesce(rc).takeFirstNValuesOrSo(sampleSize)
      else attribute
    output(o.topValues,
      sampled
        .map { case (id, value) => (value, 1) }
        .reduceByKey(_ + _)
        .top(numTopValues)(ordering)
        .toSeq
        .sorted(ordering))
  }
}

object Coverage extends OpFromJson {
  class Input extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val srcCoverage = scalar[Long]
    val dstCoverage = scalar[Long]
  }
  def fromJson(j: JsValue) = Coverage()
  def run(edges: EdgeBundle)(implicit manager: MetaGraphManager): Output = {
    import Scripting._
    val op = Coverage()
    op(op.edges, edges).result
  }
}
case class Coverage()
    extends TypedMetaGraphOp[Coverage.Input, Coverage.Output] {
  import Coverage._
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val srcs = inputs.edges.rdd.values.map(_.src)
    val dsts = inputs.edges.rdd.values.map(_.dst)
    output(o.srcCoverage, srcs.distinct.count)
    output(o.dstCoverage, dsts.distinct.count)
  }
}
