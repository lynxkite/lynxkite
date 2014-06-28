package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

case class CountVertices() extends MetaGraphOperation {
  def signature = newSignature
    .inputVertexSet('vertices)
    .outputScalar[Long]('count)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    outputs.putScalar[Long]('count, inputs.vertexSets('vertices).rdd.count)
  }
}
object CountVertices {
  def apply(metaManager: MetaGraphManager,
            dataManager: DataManager,
            vertexSet: VertexSet): Long = {
    val countMeta = metaManager
      .apply(CountVertices(), 'vertices -> vertexSet)
      .outputs
      .scalars('count).runtimeSafeCast[Long]
    dataManager.get(countMeta).value
  }
}

abstract class ComputeMinMax[T: Numeric: ClassTag] extends MetaGraphOperation {
  val MinValue: T
  val MaxValue: T
  implicit def tt: TypeTag[T]

  def signature = newSignature
    .inputVertexAttribute[T]('attribute, 'vertices, create = true)
    .outputScalar[T]('min)
    .outputScalar[T]('max)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val num = implicitly[Numeric[T]]
    val res = inputs.vertexAttributes('attribute).runtimeSafeCast[T].rdd.values
      .aggregate(Array(MaxValue, MinValue))(
        (minmax, next) => {
          minmax(0) = num.min(minmax(0), next)
          minmax(1) = num.max(minmax(1), next)
          minmax
        },
        (minmax1, minmax2) => {
          minmax1(0) = num.min(minmax1(0), minmax2(0))
          minmax1(1) = num.max(minmax1(1), minmax2(1))
          minmax1
        })
    outputs.putScalar[T]('min, res(0))
    outputs.putScalar[T]('max, res(1))
  }
}

case class ComputeMinMaxDouble() extends ComputeMinMax[Double] {
  val MinValue = Double.MinValue
  val MaxValue = Double.MaxValue
  @transient lazy val tt = typeTag[Double]
}
case class ComputeMinMaxLong() extends ComputeMinMax[Long] {
  val MinValue = Long.MinValue
  val MaxValue = Long.MaxValue
  @transient lazy val tt = typeTag[Long]
}

object ComputeMinMax {
  def apply(metaManager: MetaGraphManager,
            dataManager: DataManager,
            attr: VertexAttribute[Double]): (Double, Double) = {
    val metaOuts = metaManager.apply(ComputeMinMaxDouble(), 'attribute -> attr).outputs
    (dataManager.get(metaOuts.scalars('min).runtimeSafeCast[Double]).value,
      dataManager.get(metaOuts.scalars('max).runtimeSafeCast[Double]).value)
  }
}

abstract class ComputeTopValues[T: ClassTag](numTopValues: Int) extends MetaGraphOperation {
  implicit def tt: TypeTag[T]

  def signature = newSignature
    .inputVertexAttribute[T]('attribute, 'vertices, create = true)
    .outputScalar[Seq[(T, Int)]]('top_values)

  def execute(inputs: DataSet, outputs: DataSetBuilder, rc: RuntimeContext): Unit = {
    val iiOrdering = implicitly[Ordering[(Int, Int)]]
    object PairOrdering extends Ordering[(T, Int)] {
      def compare(a: (T, Int), b: (T, Int)) =
        iiOrdering.compare((a._2, a._1.hashCode), (b._2, b._1.hashCode))
    }
    outputs.putScalar[Seq[(T, Int)]](
      'top_values,
      inputs.vertexAttributes('attribute).runtimeSafeCast[T].rdd
        .map { case (id, value) => (value, 1) }
        .reduceByKey(_ + _)
        .top(numTopValues)(PairOrdering)
        .toSeq)
  }
}

case class ComputeTopValuesString(numTopValues: Int)
    extends ComputeTopValues[String](numTopValues) {
  @transient lazy val tt = typeTag[String]
}

object ComputeTopValues {
  def apply(metaManager: MetaGraphManager,
            dataManager: DataManager,
            attr: VertexAttribute[String],
            numTopValues: Int): Seq[(String, Int)] = {
    val metaOuts = metaManager.apply(
      ComputeTopValuesString(numTopValues), 'attribute -> attr).outputs
    dataManager.get(metaOuts.scalars('top_values).runtimeSafeCast[Seq[(String, Int)]]).value
  }
}
