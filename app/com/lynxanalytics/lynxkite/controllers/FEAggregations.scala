// Tools for creating aggregators from attributes and from the choices made on the UI.
package com.lynxanalytics.lynxkite.controllers

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_operations

trait AttributeWithLocalAggregator[From, To] {
  val attr: Attribute[From]
  val aggregator: graph_operations.LocalAggregator[From, To]
}
object AttributeWithLocalAggregator {
  def apply[From, To](
      attrInp: Attribute[From],
      aggregatorInp: graph_operations.LocalAggregator[From, To]): AttributeWithLocalAggregator[From, To] = {
    new AttributeWithLocalAggregator[From, To] {
      val attr = attrInp
      val aggregator = aggregatorInp
    }
  }
  def apply[T](
      attr: Attribute[T],
      choice: String)(
      implicit manager: MetaGraphManager): AttributeWithLocalAggregator[_, _] = {
    choice match {
      case "majority_50" =>
        AttributeWithLocalAggregator(
          attr.runtimeSafeCast[String],
          graph_operations.Aggregator.Majority(0.5))
      case "majority_100" =>
        AttributeWithLocalAggregator(
          attr.runtimeSafeCast[String],
          graph_operations.Aggregator.Majority(1.0))
      case "vector" => AttributeWithLocalAggregator(attr, graph_operations.Aggregator.AsVector[T]())
      case "set" => AttributeWithLocalAggregator(attr, graph_operations.Aggregator.AsSet[T]())
      case "median" => AttributeWithLocalAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Median())
      case _ => AttributeWithAggregator(attr, choice)
    }
  }
}

case class AttributeWithAggregator[From, Intermediate, To](
    val attr: Attribute[From],
    val aggregator: graph_operations.Aggregator[From, Intermediate, To])
    extends AttributeWithLocalAggregator[From, To]

object AttributeWithAggregator {
  def apply[T](
      attr: Attribute[T],
      choice: String)(
      implicit manager: MetaGraphManager): AttributeWithAggregator[_, _, _] = {
    def d = attr.runtimeSafeCast[Double]
    def v = attr.runtimeSafeCast[Vector[Double]]
    choice match {
      case "sum" =>
        AttributeWithAggregator(d, graph_operations.Aggregator.Sum())
      case "count" => AttributeWithAggregator(attr, graph_operations.Aggregator.Count[T]())
      case "min" =>
        AttributeWithAggregator(d, graph_operations.Aggregator.Min())
      case "max" =>
        AttributeWithAggregator(d, graph_operations.Aggregator.Max())
      case "average" => AttributeWithAggregator(
          d,
          graph_operations.Aggregator.Average())
      case "first" => AttributeWithAggregator(attr, graph_operations.Aggregator.First[T]())
      case "std_deviation" => AttributeWithAggregator(
          d,
          graph_operations.Aggregator.StdDev())
      case "most_common" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.MostCommon[T]())
      case "count_most_common" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.CountMostCommon[T]())
      case "count_distinct" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.CountDistinct[T]())
      case "elementwise_sum" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.ElementwiseSum())
      case "elementwise_min" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.ElementwiseMin())
      case "elementwise_max" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.ElementwiseMax())
      case "elementwise_average" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.ElementwiseAverage())
      case "elementwise_std_deviation" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.ElementwiseStdDev())
      case "concatenate" =>
        AttributeWithAggregator(v, graph_operations.Aggregator.Concatenate[Double]())
    }
  }
}
object AttributeWithWeightedAggregator {
  def apply[T](
      weight: Attribute[Double],
      attr: Attribute[T],
      choice: String)(
      implicit manager: MetaGraphManager): AttributeWithAggregator[_, _, _] = {

    choice match {
      case "by_max_weight" => AttributeWithAggregator(
          graph_operations.JoinAttributes.run(weight, attr),
          graph_operations.Aggregator.MaxByDouble[T]())
      case "by_min_weight" => AttributeWithAggregator(
          graph_operations.JoinAttributes.run(
            graph_operations.DeriveScala.negative(weight),
            attr),
          graph_operations.Aggregator.MaxByDouble[T]())
      case "weighted_sum" => AttributeWithAggregator(
          graph_operations.JoinAttributes.run(
            weight,
            attr.runtimeSafeCast[Double]),
          graph_operations.Aggregator.WeightedSum())
      case "weighted_average" => AttributeWithAggregator(
          graph_operations.JoinAttributes.run(
            weight,
            attr.runtimeSafeCast[Double]),
          graph_operations.Aggregator.WeightedAverage())
    }
  }
}
