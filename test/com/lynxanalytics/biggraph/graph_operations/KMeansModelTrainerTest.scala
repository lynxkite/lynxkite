package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.JavaScript

class KMeansModelTrainerTest extends FunSuite with TestGraphOp {

  test("example graph by age") {
    val sqlContext = dataManager.newSQLContext()
    import sqlContext.implicits._

    val g = ExampleGraph()().result
    // val features = (0 until 2).map { i => g.age.rdd.toDF }

    // println(g.age.rdd.sortedJoin(g.income.rdd))
    // println(features)
    // println(features.reduce((a, b) => a.join(b, "_1")).show)
    // println(g.age.toString)
    // val labels = g.vertices.rdd
    val featureNames = List("age")

    // val features = featureNames.map { name => g.age.runtimeSafeCast[Double] }
    val op = KMeansModelTrainer(2, 20, 0.0001, 1000, 1)
    val derive = op(op.features, Seq(g.age): Seq[Attribute[Double]]).result.attr
    println(derive.rdd.foreach(println))

    /*sert(bucketing.segments.toSeq.size == 2)

    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 3))
    assert(segmentSizes.sum == 4 * 1)
    assert(bucketing.label.rdd.values.collect.toSeq.sorted == Seq(0, 1))*/
  }
  /*
  test("example graph with negative values") {
    val g = ExampleGraph()().result
    val ageMinus20 = {
      val op = DeriveJSDouble(
        JavaScript("age - 20"),
        Seq("age"))
      op(
        op.attrs,
        VertexAttributeToJSValue.seq(g.age)).result.attr
    }
    // ages should be: -18, -1.8, 0.3, 30.3
    val bucketing = {
      val op = KMeansBucketing(2, 20, 0.0001, 1000, 1)
      op(op.features, Seq(ageMinus20): Seq[Attribute[Double]]).result
    }
    assert(bucketing.segments.toSeq.size == 2)
    val segmentSizes = bucketing.belongsTo.toPairSeq.groupBy(_._2).values.map(_.size).toSeq
    assert(segmentSizes.sorted == Seq(1, 3))
    assert(bucketing.label.rdd.values.collect.toSeq.sorted == Seq(0, 1))
  }*/

}
