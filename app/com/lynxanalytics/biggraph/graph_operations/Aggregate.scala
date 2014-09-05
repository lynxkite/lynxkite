package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.RDDUtils

object AggregateByEdgeBundle {
  class Input[From] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val connection = edgeBundle(src, dst)
    val attr = vertexAttribute[From](src)
  }
  class Output[From, To: TypeTag](implicit instance: MetaGraphOperationInstance,
                                  inputs: Input[From]) extends MagicOutput(instance) {
    val attr = vertexAttribute[To](inputs.dst.entity)
  }
}
case class AggregateByEdgeBundle[From, To](aggregator: LocalAggregator[From, To])
    extends TypedMetaGraphOp[AggregateByEdgeBundle.Input[From], AggregateByEdgeBundle.Output[From, To]] {
  import AggregateByEdgeBundle._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[From]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    val tt = aggregator.outputTypeTag(inputs.attr.typeTag)
    new Output[From, To]()(tt, instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag

    val bySrc = inputs.connection.rdd.map {
      case (id, edge) => edge.src -> edge.dst
    }.toSortedRDD(inputs.src.rdd.partitioner.get)
    val withAttr = bySrc.sortedJoin(inputs.attr.rdd)
    val byDst = withAttr.map {
      case (src, (dst, attr)) => dst -> attr
    }
    val grouped = byDst.groupBySortedKey(inputs.dst.rdd.partitioner.get)
    val aggregated = grouped.mapValues(aggregator.aggregate(_))
    output(o.attr, aggregated)
  }
}

object AggregateFromEdges {
  class Input[From] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val ids = vertexSet
    val edges = edgeBundle(src, dst, idSet = ids)
    val eattr = vertexAttribute[From](ids)
  }
  class Output[From, To: TypeTag](implicit instance: MetaGraphOperationInstance,
                                  inputs: Input[From]) extends MagicOutput(instance) {
    val srcAttr = vertexAttribute[To](inputs.src.entity)
    val dstAttr = vertexAttribute[To](inputs.dst.entity)
  }
}
case class AggregateFromEdges[From, To](aggregator: LocalAggregator[From, To])
    extends TypedMetaGraphOp[AggregateFromEdges.Input[From], AggregateFromEdges.Output[From, To]] {
  import AggregateFromEdges._
  override val isHeavy = true
  @transient override lazy val inputs = new Input[From]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    val tt = aggregator.outputTypeTag(inputs.eattr.typeTag)
    new Output[From, To]()(tt, instance, inputs)
  }

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.eattr.data.classTag

    val src = inputs.src.rdd
    val dst = inputs.dst.rdd
    val edges = inputs.edges.rdd
    val eattr = inputs.eattr.rdd
    val edgesWAttr = edges.sortedJoin(eattr)
    val bySrc = edgesWAttr.map {
      case (eid, (edge, value)) => edge.src -> value
    }.groupBySortedKey(src.partitioner.get)
    val byDst = edgesWAttr.map {
      case (eid, (edge, value)) => edge.dst -> value
    }.groupBySortedKey(dst.partitioner.get)
    output(o.srcAttr, bySrc.mapValues(aggregator.aggregate(_)))
    output(o.dstAttr, byDst.mapValues(aggregator.aggregate(_)))
  }
}

object AggregateAttributeToScalar {
  class Output[To: TypeTag](
      implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {

    val aggregated = scalar[To]
  }
}

case class AggregateAttributeToScalar[From, Intermediate, To](
  aggregator: Aggregator[From, Intermediate, To])
    extends TypedMetaGraphOp[VertexAttributeInput[From], AggregateAttributeToScalar.Output[To]] {
  import AggregateAttributeToScalar._
  override val isHeavy = true
  @transient override lazy val inputs = new VertexAttributeInput[From]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    val tt = aggregator.outputTypeTag(inputs.attr.typeTag)
    new Output[To]()(tt, instance)
  }

  def execute(inputDatas: DataSet,
              o: Output[To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val attr = inputs.attr.rdd
    implicit val ftt = inputs.attr.data.typeTag
    implicit val fct = inputs.attr.data.classTag
    implicit val ict = RuntimeSafeCastable.classTagFromTypeTag(aggregator.intermediateTypeTag(ftt))
    output(
      o.aggregated,
      aggregator.finalize(
        attr
          .values
          .mapPartitions(it => Iterator(aggregator.aggregatePartition(it)))
          .collect
          .foldLeft(aggregator.zero)(aggregator.combine _)))
  }
}

trait LocalAggregator[From, To] {
  def outputTypeTag(inputTypeTag: TypeTag[From]): TypeTag[To]
  // aggregate() can assume that values is non-empty.
  def aggregate(values: Iterable[From]): To
}
trait Aggregator[From, Intermediate, To] extends LocalAggregator[From, To] {
  def intermediateTypeTag(inputTypeTag: TypeTag[From]): TypeTag[Intermediate]
  def zero: Intermediate
  def merge(a: Intermediate, b: From): Intermediate
  def combine(a: Intermediate, b: Intermediate): Intermediate
  def finalize(i: Intermediate): To
  def aggregatePartition(values: Iterator[From]): Intermediate =
    values.foldLeft(zero)(merge _)
  def aggregate(values: Iterable[From]): To =
    finalize(aggregatePartition(values.iterator))
}
trait SimpleAggregator[From, To] extends Aggregator[From, To, To] {
  def finalize(i: To): To = i
  def intermediateTypeTag(inputTypeTag: TypeTag[From]) = outputTypeTag(inputTypeTag)
}
// This is a trait instead of an abstract class because otherwise the case
// class will not be serializable ("no valid constructor").
trait CompoundAggregator[From, Intermediate1, Intermediate2, To1, To2, To]
    extends Aggregator[From, (Intermediate1, Intermediate2), To] {
  val agg1: Aggregator[From, Intermediate1, To1]
  val agg2: Aggregator[From, Intermediate2, To2]
  def zero = (agg1.zero, agg2.zero)
  def merge(a: (Intermediate1, Intermediate2), b: From) =
    (agg1.merge(a._1, b), agg2.merge(a._2, b))
  def combine(a: (Intermediate1, Intermediate2), b: (Intermediate1, Intermediate2)) =
    (agg1.combine(a._1, b._1), agg2.combine(a._2, b._2))
  def finalize(i: (Intermediate1, Intermediate2)): To =
    compound(agg1.finalize(i._1), agg2.finalize(i._2))
  def compound(res1: To1, res2: To2): To
  def intermediateTypeTag(inputTypeTag: TypeTag[From]): TypeTag[(Intermediate1, Intermediate2)] = {
    implicit val tt1 = agg1.intermediateTypeTag(inputTypeTag)
    implicit val tt2 = agg2.intermediateTypeTag(inputTypeTag)
    typeTag[(Intermediate1, Intermediate2)]
  }
}

object Aggregator {
  case class Count[T]() extends SimpleAggregator[T, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: T) = a + 1
    def combine(a: Double, b: Double) = a + b
  }

  case class Sum() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: Double) = a + b
    def combine(a: Double, b: Double) = a + b
  }

  case class WeightedSum() extends SimpleAggregator[(Double, Double), Double] {
    def outputTypeTag(inputTypeTag: TypeTag[(Double, Double)]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: (Double, Double)) = a + b._1 * b._2
    def combine(a: Double, b: Double) = a + b
  }

  case class Max() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = Double.NegativeInfinity
    def merge(a: Double, b: Double) = math.max(a, b)
    def combine(a: Double, b: Double) = math.max(a, b)
  }

  case class Min() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = Double.PositiveInfinity
    def merge(a: Double, b: Double) = math.min(a, b)
    def combine(a: Double, b: Double) = math.min(a, b)
  }

  def typeToTypeTag[T](tpe: Type, mirror: reflect.api.Mirror[reflect.runtime.universe.type]): TypeTag[T] = {
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        if (m eq mirror) tpe.asInstanceOf[U#Type]
        else throw new IllegalArgumentException(
          s"Type tag defined in $mirror cannot be migrated to other mirrors.")
      }
    })
  }

  def typeArgs(tpe: Type): Seq[Type] = tpe.asInstanceOf[TypeRefApi].args

  case class MaxBy[Weight: Ordering, Value]() extends Aggregator[(Weight, Value), Option[(Weight, Value)], Value] {
    import Ordering.Implicits._
    def optionTypeTag[T: TypeTag] = typeTag[Option[T]]
    def intermediateTypeTag(inputTypeTag: TypeTag[(Weight, Value)]) = {
      implicit val tt = inputTypeTag
      optionTypeTag[(Weight, Value)]
    }
    def outputTypeTag(inputTypeTag: TypeTag[(Weight, Value)]) = {
      implicit val tt = typeToTypeTag[Value](typeArgs(inputTypeTag.tpe).last, inputTypeTag.mirror)
      typeTag[Value]
    }
    def zero = None
    def merge(aOpt: Option[(Weight, Value)], b: (Weight, Value)) = {
      aOpt match {
        case Some(a) => if (a._1 < b._1) Some(b) else Some(a)
        case None => Some(b)
      }
    }
    def combine(aOpt: Option[(Weight, Value)], bOpt: Option[(Weight, Value)]) = {
      (aOpt, bOpt) match {
        case (Some(a), Some(b)) => if (a._1 < b._1) Some(b) else Some(a)
        case _ => aOpt.orElse(bOpt)
      }
    }
    def finalize(opt: Option[(Weight, Value)]) = opt.get._2
  }

  case class Average() extends CompoundAggregator[Double, Double, Double, Double, Double, Double] {
    val agg1 = Count[Double]()
    val agg2 = Sum()
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def compound(count: Double, sum: Double) = sum / count
  }

  case class WeightedAverage() extends CompoundAggregator[(Double, Double), Double, Double, Double, Double, Double] {
    val agg1 = Count[(Double, Double)]()
    val agg2 = WeightedSum()
    def outputTypeTag(inputTypeTag: TypeTag[(Double, Double)]) = typeTag[Double]
    def compound(count: Double, sum: Double) = sum / count
  }

  case class MostCommon[T]() extends LocalAggregator[T, T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def aggregate(values: Iterable[T]) = {
      values.groupBy(identity).maxBy(_._2.size)._1
    }
  }

  // Majority is like MostCommon, but returns "" if the mode is < fraction of the values.
  case class Majority(fraction: Double) extends LocalAggregator[String, String] {
    def outputTypeTag(inputTypeTag: TypeTag[String]) = typeTag[String]
    def aggregate(values: Iterable[String]) = {
      val (mode, count) = values.groupBy(identity).mapValues(_.size).maxBy(_._2)
      if (count >= fraction * values.size) mode else ""
    }
  }

  case class First[T]() extends Aggregator[T, Option[T], T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def intermediateTypeTag(inputTypeTag: TypeTag[T]): TypeTag[Option[T]] = {
      implicit val tt = inputTypeTag
      typeTag[Option[T]]
    }
    def zero = None
    def merge(a: Option[T], b: T) = a.orElse(Some(b))
    def combine(a: Option[T], b: Option[T]) = a.orElse(b)
    def finalize(opt: Option[T]) = opt.get
  }

  case class AsVector[T]() extends LocalAggregator[T, Vector[T]] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = {
      implicit val tt = inputTypeTag
      typeTag[Vector[T]]
    }
    def aggregate(values: Iterable[T]): Vector[T] = values.toVector
  }
}
