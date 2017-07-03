// All the aggregation operations and the aggregators.
//
// The operations use a given aggregator to aggregate an attribute.
// For example the operation AggregateAttributeToScalar with the aggregator
// Aggregator.Average can calculate the global average of the attribute.

package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark
import org.apache.spark.rdd.RDD

object AggregateByEdgeBundle extends OpFromJson {
  class Input[From] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val connection = edgeBundle(src, dst)
    val bySrc = hybridBundle(connection)
    val attr = vertexAttribute[From](src)
  }
  class Output[From, To: TypeTag](implicit instance: MetaGraphOperationInstance,
                                  inputs: Input[From]) extends MagicOutput(instance) {
    val attr = vertexAttribute[To](inputs.dst.entity)
  }
  def fromJson(j: JsValue) =
    AggregateByEdgeBundle(TypedJson.read[LocalAggregator[_, _]](j \ "aggregator"))
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
  override def toJson = Json.obj("aggregator" -> aggregator.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ftt = inputs.attr.data.typeTag
    implicit val fct = inputs.attr.data.classTag
    implicit val runtimeContext = rc
    val es = inputs.bySrc.rdd

    val partitioner = es.partitioner.get
    val withAttr = es.lookup(inputs.attr.rdd.sortedRepartition(partitioner))
    val byDst = withAttr.map {
      case (_, (dst, attr)) => dst -> attr
    }
    aggregator match {
      case aggregator: Aggregator[From, _, To] =>
        // Scalable aggregation for non-local Aggregators.
        val aggregated = aggregator.aggregateRDD(byDst, inputs.dst.rdd.partitioner.get)
        output(o.attr, aggregated)
      case _ =>
        // Regular aggregation for local Aggregators.
        val grouped = byDst.groupBySortedKey(inputs.dst.rdd.partitioner.get)
        val aggregated = grouped.mapValues(aggregator.aggregate(_))
        output(o.attr, aggregated)
    }
  }
}

object AggregateFromEdges extends OpFromJson {
  class Input[From] extends MagicInputSignature {
    val src = vertexSet
    val dst = vertexSet
    val edges = edgeBundle(src, dst)
    val eattr = edgeAttribute[From](edges)
  }
  class Output[From, To: TypeTag](implicit instance: MetaGraphOperationInstance,
                                  inputs: Input[From]) extends MagicOutput(instance) {
    val dstAttr = vertexAttribute[To](inputs.dst.entity)
  }
  def fromJson(j: JsValue) =
    AggregateFromEdges(TypedJson.read[LocalAggregator[_, _]](j \ "aggregator"))
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
  override def toJson = Json.obj("aggregator" -> aggregator.toTypedJson)

  def execute(inputDatas: DataSet,
              o: Output[From, To],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ftt = inputs.eattr.data.typeTag
    implicit val fct = inputs.eattr.data.classTag

    val dst = inputs.dst.rdd
    val edges = inputs.edges.rdd
    val eattr = inputs.eattr.rdd
    val edgesWAttr = edges.sortedJoin(eattr)
    val byDst = edgesWAttr.map {
      case (eid, (edge, value)) => edge.dst -> value
    }
    aggregator match {
      case aggregator: Aggregator[From, _, To] =>
        // Scalable aggregation for non-local Aggregators.
        output(o.dstAttr, aggregator.aggregateRDD(byDst, inputs.dst.rdd.partitioner.get))
      case _ =>
        // Regular aggregation for local Aggregators.
        output(o.dstAttr,
          byDst.groupBySortedKey(dst.partitioner.get).mapValues(aggregator.aggregate(_)))
    }
  }
}

object AggregateAttributeToScalar extends OpFromJson {
  class Output[To: TypeTag](
      implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {

    val aggregated = scalar[To]
  }
  def fromJson(j: JsValue): TypedMetaGraphOp.Type =
    AggregateAttributeToScalar(TypedJson.read[Aggregator[_, _, _]](j \ "aggregator"))
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
  override def toJson = Json.obj("aggregator" -> aggregator.toTypedJson)

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

// A simple interface that does not cover distributed use.
trait LocalAggregator[From, To] extends ToJson {
  def outputTypeTag(inputTypeTag: TypeTag[From]): TypeTag[To]
  // aggregate() can assume that values is non-empty.
  def aggregate(values: Iterable[From]): To

  // Aggregates all values belonging to the same key using this aggregator.
  def aggregateByKey[K](input: Seq[(K, From)]): Map[K, To] = {
    val groupped = input.groupBy(_._1)
    groupped.mapValues(group => aggregate(group.map(_._2)))
  }
}
// Aggregates from From to Intermediate and at the end calls finalize() to turn
// Intermediate into To. So Intermediate can contain extra data over what is
// required in the result. Provides a scalable aggregateRDD() method.
trait Aggregator[From, Intermediate, To] extends LocalAggregator[From, To] {
  def intermediateTypeTag(inputTypeTag: TypeTag[From]): TypeTag[Intermediate]
  def zero: Intermediate
  def combine(a: Intermediate, b: Intermediate): Intermediate
  def finalize(i: Intermediate): To
  def aggregatePartition(values: Iterator[From]): Intermediate
  def aggregate(values: Iterable[From]): To =
    finalize(aggregatePartition(values.iterator))
  // Aggregates the RDD by key in a scalable (hotspot resistant) way.
  def aggregateRDD[K: Ordering: ClassTag](
    values: RDD[(K, From)], partitioner: spark.Partitioner)(implicit ftt: TypeTag[From]): UniqueSortedRDD[K, To]
}

// The trivial extension of Aggregator.
trait ItemAggregator[From, Intermediate, To] extends Aggregator[From, Intermediate, To] {
  def merge(a: Intermediate, b: From): Intermediate
  def aggregatePartition(values: Iterator[From]): Intermediate =
    values.foldLeft(zero)(merge _)
  // Aggregates the RDD by key in a scalable (hotspot resistant) way.
  def aggregateRDD[K: Ordering: ClassTag](
    values: RDD[(K, From)], partitioner: spark.Partitioner)(implicit ftt: TypeTag[From]): UniqueSortedRDD[K, To] = {
    implicit val ict = RuntimeSafeCastable.classTagFromTypeTag(intermediateTypeTag(ftt))
    implicit val fct = RuntimeSafeCastable.classTagFromTypeTag(ftt)
    values.aggregateBySortedKey[Intermediate](zero, partitioner)(merge, combine).mapValues { i => finalize(i) }
  }
}

// A two phase aggregator which creates an RDD of the counts of occurrences for each value
// per key (key -> (count, value)). The merge method acts on this RDD. Similarly the
// aggregatePartition method creates a list of (value, count)-s to merge. The occurrences of
// distinct (key, value) pairs and the occurrence of each value within a key should lead to the
// same result.
trait CountAggregator[From, Intermediate, To] extends Aggregator[From, Intermediate, To] {
  def merge(a: Intermediate, b: (Double, From)): Intermediate
  def aggregatePartition(values: Iterator[From]): Intermediate = values
    .toIterable
    .groupBy(identity)
    .map { case (v, i) => (i.size.toDouble, v) }
    .foldLeft(zero)(merge _)
  // Aggregates the RDD by key in a scalable (hotspot resistant) way.
  def aggregateRDD[K: Ordering: ClassTag](
    values: RDD[(K, From)], partitioner: spark.Partitioner)(implicit ftt: TypeTag[From]): UniqueSortedRDD[K, To] = {
    implicit val ict = RuntimeSafeCastable.classTagFromTypeTag(intermediateTypeTag(ftt))
    implicit val fct = RuntimeSafeCastable.classTagFromTypeTag(ftt)
    values
      .map { case (k, v) => (k, v) -> 1.0 }
      .reduceByKey(_ + _)
      .map { case ((k, v), c) => k -> (c, v) }
      .aggregateBySortedKey[Intermediate](zero, partitioner)(merge, combine)
      .mapValues { i => finalize(i) }
  }
}

// A distributed aggregator where Intermediate is not different from To.
trait SimpleAggregator[From, To] extends ItemAggregator[From, To, To] {
  def finalize(i: To): To = i
  def intermediateTypeTag(inputTypeTag: TypeTag[From]) = outputTypeTag(inputTypeTag)
}
// CompoundAggregator combines two aggregators. Only compound() and
// outputTypeTag() need to be implemented.
// This is a trait instead of an abstract class because otherwise the case
// class will not be serializable ("no valid constructor").
trait CompoundAggregator[From, Intermediate1, Intermediate2, To1, To2, To]
    extends ItemAggregator[From, (Intermediate1, Intermediate2), To] {
  val agg1: ItemAggregator[From, Intermediate1, To1]
  val agg2: ItemAggregator[From, Intermediate2, To2]
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
// A convenient shorthand.
trait CompoundDoubleAggregator[From]
    extends CompoundAggregator[From, Double, Double, Double, Double, Double] {
  def outputTypeTag(inputTypeTag: TypeTag[From]) = typeTag[Double]
}

object Aggregator {
  // Type aliases for the JSON serialization.
  type AnyAggregator = Aggregator[_, _, _]
  type AnyLocalAggregator = LocalAggregator[_, _]
  type AggregatorFromJson = FromJson[AnyAggregator]
  type LocalAggregatorFromJson = FromJson[AnyLocalAggregator]

  object Count extends AggregatorFromJson { def fromJson(j: JsValue) = Count() }
  case class Count[T]() extends SimpleAggregator[T, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: T) = a + 1
    def combine(a: Double, b: Double) = a + b
  }

  object Sum extends AggregatorFromJson { def fromJson(j: JsValue) = Sum() }
  case class Sum() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: Double) = a + b
    def combine(a: Double, b: Double) = a + b
  }

  object WeightedSum extends AggregatorFromJson { def fromJson(j: JsValue) = WeightedSum() }
  case class WeightedSum() extends SimpleAggregator[(Double, Double), Double] {
    def outputTypeTag(inputTypeTag: TypeTag[(Double, Double)]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: (Double, Double)) = a + b._1 * b._2
    def combine(a: Double, b: Double) = a + b
  }

  object Max extends AggregatorFromJson { def fromJson(j: JsValue) = Max() }
  case class Max() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = Double.NegativeInfinity
    def merge(a: Double, b: Double) = math.max(a, b)
    def combine(a: Double, b: Double) = math.max(a, b)
  }

  object Min extends AggregatorFromJson { def fromJson(j: JsValue) = Min() }
  case class Min() extends SimpleAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = typeTag[Double]
    def zero = Double.PositiveInfinity
    def merge(a: Double, b: Double) = math.min(a, b)
    def combine(a: Double, b: Double) = math.min(a, b)
  }

  abstract class MaxBy[Weight: Ordering, Value]
      extends ItemAggregator[(Weight, Value), Option[(Weight, Value)], Value] with Serializable {
    import Ordering.Implicits._
    def intermediateTypeTag(inputTypeTag: TypeTag[(Weight, Value)]) = {
      implicit val tt = inputTypeTag
      // The intermediate type is Option[(Weight, Value)], which is None for empty input and
      // Some(maximal element) otherwise.
      TypeTagUtil.optionTypeTag[(Weight, Value)]
    }
    def outputTypeTag(inputTypeTag: TypeTag[(Weight, Value)]) =
      TypeTagUtil.typeArgs(inputTypeTag).last.asInstanceOf[TypeTag[Value]]
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
  object MaxByDouble extends AggregatorFromJson { def fromJson(j: JsValue) = MaxByDouble() }
  case class MaxByDouble[T]() extends MaxBy[Double, T]

  object Average extends AggregatorFromJson { def fromJson(j: JsValue) = Average() }
  case class Average() extends CompoundDoubleAggregator[Double] {
    val agg1 = Count[Double]()
    val agg2 = Sum()
    def compound(count: Double, sum: Double) = {
      assert(count != 0, "Average of empty set")
      sum / count
    }
  }

  object SumOfWeights extends AggregatorFromJson { def fromJson(j: JsValue) = SumOfWeights() }
  case class SumOfWeights[T]() extends SimpleAggregator[(Double, T), Double] {
    def outputTypeTag(inputTypeTag: TypeTag[(Double, T)]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: (Double, T)) = a + b._1
    def combine(a: Double, b: Double) = a + b
  }

  object WeightedAverage extends AggregatorFromJson { def fromJson(j: JsValue) = WeightedAverage() }
  case class WeightedAverage() extends CompoundDoubleAggregator[(Double, Double)] {
    val agg1 = SumOfWeights[Double]()
    val agg2 = WeightedSum()
    def compound(weights: Double, weightedSum: Double) = {
      assert(weights != 0, "Average of 0 weight set")
      weightedSum / weights
    }
  }

  object Median extends LocalAggregatorFromJson { def fromJson(j: JsValue) = Median() }
  case class Median() extends LocalAggregator[Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = inputTypeTag
    def aggregate(values: Iterable[Double]) = {
      val cnt: Int = values.size
      val (halfCount1, halfCount2) =
        if (cnt % 2 == 0) (cnt / 2 - 1, cnt / 2)
        else ((cnt - 1) / 2, (cnt - 1) / 2)
      val sortedValues = values.toSeq.sorted
      (sortedValues(halfCount1) + sortedValues(halfCount2)) / 2
    }
  }

  object MostCommon extends AggregatorFromJson { def fromJson(j: JsValue) = MostCommon() }
  case class MostCommon[T]() extends CountAggregator[T, Option[(Double, T)], T] {
    val agg = MaxByDouble[T]
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def intermediateTypeTag(inputTypeTag: TypeTag[T]) = {
      implicit val tt = inputTypeTag
      // The intermediate type is Option[(Weight, Value)], which is None for empty input and
      // Some(maximal element) otherwise.
      TypeTagUtil.optionTypeTag[(Double, T)]
    }
    def zero = None
    def merge(a: Option[(Double, T)], b: (Double, T)) = { agg.merge(a, b) }
    def combine(a: Option[(Double, T)], b: Option[(Double, T)]) = { agg.combine(a, b) }
    def finalize(i: Option[(Double, T)]): T = { agg.finalize(i) }
  }

  object CountMostCommon extends AggregatorFromJson { def fromJson(j: JsValue) = CountMostCommon() }
  case class CountMostCommon[T]() extends CountAggregator[T, Option[(Double, T)], Double] {
    val agg = MaxByDouble[T]
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def intermediateTypeTag(inputTypeTag: TypeTag[T]) = {
      implicit val tt = inputTypeTag
      // The intermediate type is Option[(Weight, Value)], which is None for empty input and
      // Some(maximal element) otherwise.
      TypeTagUtil.optionTypeTag[(Double, T)]
    }
    def zero = None
    def merge(a: Option[(Double, T)], b: (Double, T)) = { agg.merge(a, b) }
    def combine(a: Option[(Double, T)], b: Option[(Double, T)]) = { agg.combine(a, b) }
    def finalize(i: Option[(Double, T)]): Double = i.get._1
  }

  object CountDistinct extends AggregatorFromJson { def fromJson(j: JsValue) = CountDistinct() }
  case class CountDistinct[T]() extends CountAggregator[T, Double, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def intermediateTypeTag(inputTypeTag: TypeTag[T]) = typeTag[Double]
    def zero = 0
    def merge(a: Double, b: (Double, T)) = a + 1
    def combine(a: Double, b: Double) = a + b
    def finalize(i: Double): Double = i
  }

  // Majority is like MostCommon, but returns "" if the mode is < fraction of the values.
  object Majority extends LocalAggregatorFromJson {
    def fromJson(j: JsValue) = Majority((j \ "fraction").as[Double])
  }
  case class Majority(fraction: Double) extends LocalAggregator[String, String] {
    override def toJson = Json.obj("fraction" -> fraction)
    def outputTypeTag(inputTypeTag: TypeTag[String]) = typeTag[String]
    def aggregate(values: Iterable[String]) = {
      val (mode, count) = values.groupBy(identity).mapValues(_.size).maxBy(_._2)
      if (count >= fraction * values.size) mode else ""
    }
  }

  object First extends AggregatorFromJson { def fromJson(j: JsValue) = First() }
  case class First[T]() extends ItemAggregator[T, Option[T], T] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = inputTypeTag
    def intermediateTypeTag(inputTypeTag: TypeTag[T]): TypeTag[Option[T]] = {
      implicit val tt = inputTypeTag
      typeTag[Option[T]]
    }
    def zero = None
    def merge(a: Option[T], b: T) = a.orElse(Some(b))
    def combine(a: Option[T], b: Option[T]) = a.orElse(b)
    def finalize(opt: Option[T]) = {
      assert(opt.nonEmpty, "Average of 0 weight set")
      opt.get
    }
  }

  object AsVector extends LocalAggregatorFromJson { def fromJson(j: JsValue) = AsVector() }
  case class AsVector[T]() extends LocalAggregator[T, Vector[T]] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = {
      implicit val tt = inputTypeTag
      typeTag[Vector[T]]
    }
    def aggregate(values: Iterable[T]): Vector[T] = values.toVector
  }

  object AsSet extends LocalAggregatorFromJson { def fromJson(j: JsValue) = AsSet() }
  case class AsSet[T]() extends LocalAggregator[T, Set[T]] {
    def outputTypeTag(inputTypeTag: TypeTag[T]) = {
      implicit val tt = inputTypeTag
      typeTag[Set[T]]
    }
    def aggregate(values: Iterable[T]): Set[T] = values.toSet
  }

  object StdDev extends AggregatorFromJson { def fromJson(j: JsValue) = StdDev() }
  case class StdDev() extends ItemAggregator[Double, Stats, Double] {
    def outputTypeTag(inputTypeTag: TypeTag[Double]) = inputTypeTag
    def intermediateTypeTag(inputTypeTag: TypeTag[Double]): TypeTag[Stats] = typeTag[Stats]
    def zero = Stats(0, 0, 0)
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Incremental_algorithm
    def merge(a: Stats, b: Double) = {
      val n = a.n + 1
      val delta = b - a.mean
      val mean = a.mean + delta / n
      val sigma = a.sigma + delta * (b - mean) // = a.sigma + delta * delta * (n-1) / n
      Stats(n, mean, sigma)
    }
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    // http://i.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf
    def combine(a: Stats, b: Stats) = {
      val n = a.n + b.n
      val delta = b.mean - a.mean
      val mean = (a.n * a.mean + b.n * b.mean) / n
      val sigma = a.sigma + b.sigma + (delta * delta * a.n * b.n) / n
      Stats(n, mean, sigma)
    }
    // we drop count and mean, calculate standard deviation from variance
    // TODO: I leave the class name as variance as later we intend to output all 4 possible outputs
    // TODO: for global aggregation (not on samples) we should do Math.sqrt(a.sigma / a.n)
    def finalize(a: Stats) = if (a.n < 2) 0 else Math.sqrt(a.sigma / (a.n - 1))
  }
}

// sigma: sum of squares of differences from the mean
case class Stats(n: Long, mean: Double, sigma: Double)
