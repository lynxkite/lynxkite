// Tools for creating aggregators from attributes and from the choices made on the UI.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations

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
    attr: Attribute[T], choice: String)(
      implicit manager: MetaGraphManager): AttributeWithLocalAggregator[_, _] = {
    choice match {
      case "majority_50" =>
        AttributeWithLocalAggregator(
          attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(0.5))
      case "majority_100" =>
        AttributeWithLocalAggregator(
          attr.runtimeSafeCast[String], graph_operations.Aggregator.Majority(1.0))
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
    attr: Attribute[T], choice: String)(
      implicit manager: MetaGraphManager): AttributeWithAggregator[_, _, _] = {

    choice match {
      case "sum" =>
        AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Sum())
      case "count" => AttributeWithAggregator(attr, graph_operations.Aggregator.Count[T]())
      case "min" =>
        AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Min())
      case "max" =>
        AttributeWithAggregator(attr.runtimeSafeCast[Double], graph_operations.Aggregator.Max())
      case "average" => AttributeWithAggregator(
        attr.runtimeSafeCast[Double], graph_operations.Aggregator.Average())
      case "first" => AttributeWithAggregator(attr, graph_operations.Aggregator.First[T]())
      case "std_deviation" => AttributeWithAggregator(
        attr.runtimeSafeCast[Double], graph_operations.Aggregator.StdDev())
      case "most_common" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.MostCommon[T]())
      case "count_most_common" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.CountMostCommon[T]())
      case "count_distinct" =>
        AttributeWithAggregator(attr, graph_operations.Aggregator.CountDistinct[T]())
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
          graph_operations.DeriveScala.negative(weight), attr),
        graph_operations.Aggregator.MaxByDouble[T]())
      case "weighted_sum" => AttributeWithAggregator(
        graph_operations.JoinAttributes.run(
          weight, attr.runtimeSafeCast[Double]), graph_operations.Aggregator.WeightedSum())
      case "weighted_average" => AttributeWithAggregator(
        graph_operations.JoinAttributes.run(
          weight, attr.runtimeSafeCast[Double]), graph_operations.Aggregator.WeightedAverage())
    }
  }
}
