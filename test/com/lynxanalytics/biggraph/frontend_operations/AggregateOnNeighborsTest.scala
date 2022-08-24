package com.lynxanalytics.biggraph.frontend_operations

import scala.reflect.runtime.universe.TypeTag
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class AggregateOnNeighborsTest extends OperationsTestBase {
  test("all aggregators") {
    def agg[T](attribute: String, aggregator: String): Map[Long, T] = {
      val p = box("Create example graph")
        .box(
          "Aggregate on neighbors",
          Map(
            "prefix" -> "",
            "direction" -> "all edges",
            ("aggregate_" + attribute) -> aggregator))
        .project
      get(p.vertexAttributes(attribute + "_" + aggregator)).asInstanceOf[Map[Long, T]]
    }
    assert(agg("gender", "count") == Map(0 -> 3.0, 1 -> 3.0, 2 -> 2.0))
    assert(agg("gender", "count_distinct") == Map(0 -> 2.0, 1 -> 1.0, 2 -> 2.0))
    assert(agg("gender", "most_common") == Map(0 -> "Female", 1 -> "Male", 2 -> "Male"))
    assert(agg("gender", "count_most_common") == Map(0 -> 2.0, 1 -> 3.0, 2 -> 1.0))
    assert(agg("gender", "first") == Map(0 -> "Female", 1 -> "Male", 2 -> "Male"))
    assert(agg("gender", "set") == Map(
      0 -> Set("Female", "Male"),
      1 -> Set("Male"),
      2 -> Set("Male", "Female")))
    assert(agg("gender", "vector") == Map(
      0 -> Vector("Female", "Female", "Male"),
      1 -> Vector("Male", "Male", "Male"),
      2 -> Vector("Male", "Female")))
    assert(agg[Double]("age", "average").mapValues(_.round) == Map(0 -> 29, 1 -> 30, 2 -> 19))
    assert(agg[Double]("age", "min").mapValues(_.round) == Map(0 -> 18, 1 -> 20, 2 -> 18))
    assert(agg[Double]("age", "max").mapValues(_.round) == Map(0 -> 50, 1 -> 50, 2 -> 20))
    assert(agg[Double]("age", "median").mapValues(_.round) == Map(0 -> 18, 1 -> 20, 2 -> 19))
    assert(agg[Double]("age", "std_deviation").mapValues(_.round) == Map(0 -> 19, 1 -> 17, 2 -> 1))
    assert(agg[Double]("age", "sum").mapValues(_.round) == Map(0 -> 87, 1 -> 91, 2 -> 39))
    assert(agg[Vector[Double]]("location", "elementwise_average").mapValues(v => v.map(_.round))
      == Map(0 -> Vector(32, 47), 1 -> Vector(28, -15), 2 -> Vector(44, -27)))
    assert(agg[Vector[Double]]("location", "elementwise_sum").mapValues(v => v.map(_.round))
      == Map(0 -> Vector(96, 142), 1 -> Vector(83, -44), 2 -> Vector(88, -55)))
    assert(agg[Vector[Double]]("location", "elementwise_min").mapValues(v => v.map(_.round))
      == Map(0 -> Vector(1, 19), 1 -> Vector(1, -74), 2 -> Vector(41, -74)))
    assert(agg[Vector[Double]]("location", "elementwise_max").mapValues(v => v.map(_.round))
      == Map(0 -> Vector(48, 104), 1 -> Vector(41, 104), 2 -> Vector(48, 19)))
    assert(agg[Vector[Double]]("location", "elementwise_std_deviation").mapValues(v => v.map(_.round))
      == Map(0 -> Vector(27, 49), 1 -> Vector(23, 103), 2 -> Vector(5, 66)))
    assert(agg[Vector[Double]]("location", "concatenate").mapValues(v => v.map(_.round)) == Map(
      0 -> Vector(48, 19, 48, 19, 1, 104),
      1 -> Vector(41, -74, 41, -74, 1, 104),
      2 -> Vector(41, -74, 48, 19)))
  }
}

class WeightedAggregateOnNeighborsTest extends OperationsTestBase {
  test("all aggregators") {
    def agg[T: TypeTag](attribute: String, aggregator: String, weight: String): Map[Long, T] = {
      val p = box("Create example graph")
        .box(
          "Weighted aggregate on neighbors",
          Map(
            "prefix" -> "",
            "direction" -> "all edges",
            "weight" -> weight,
            ("aggregate_" + attribute) -> aggregator))
        .project
      get(p.vertexAttributes(attribute + "_" + aggregator + "_by_" + weight).runtimeSafeCast[T])
    }
    assert(agg[Double]("age", "weighted_average", "age").mapValues(_.round) == Map(0 -> 37, 1 -> 37, 2 -> 19))
    assert(agg[Double]("age", "by_max_weight", "age").mapValues(_.round) == Map(0 -> 50, 1 -> 50, 2 -> 20))
    assert(agg[Double]("age", "by_min_weight", "age").mapValues(_.round) == Map(0 -> 18, 1 -> 20, 2 -> 18))
    assert(agg[Double]("age", "weighted_sum", "age").mapValues(_.round) == Map(0 -> 3193, 1 -> 3354, 2 -> 743))
    assert(agg[Double]("age", "weighted_average", "weight").mapValues(_.round) == Map(0 -> 34, 1 -> 37, 2 -> 19))
    assert(agg[Double]("age", "by_max_weight", "weight").mapValues(_.round) == Map(0 -> 50, 1 -> 50, 2 -> 18))
    assert(agg[Double]("age", "by_min_weight", "weight").mapValues(_.round) == Map(0 -> 18, 1 -> 20, 2 -> 20))
    assert(agg[Double]("age", "weighted_sum", "weight").mapValues(_.round) == Map(0 -> 205, 1 -> 262, 2 -> 134))
  }
}
