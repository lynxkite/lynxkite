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
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.JavaScript

class ReduceDimensionsTest extends FunSuite with TestGraphOp {

  test("example graph") {
    val sqlContext = dataManager.newSQLContext()
    import sqlContext.implicits._

    val g = ExampleGraph()().result
    val op = ReduceDimensions(2)
    val result = op(op.features, Seq(g.age, g.income): Seq[Attribute[Double]]).result
    val attr1 = result.attr1.rdd
    val attr2 = result.attr2.rdd
    assert(attr1.count == 2)
    assert(attr2.count == 2)
  }

  test("larger data set with 10 attributes") {
    val sqlContext = dataManager.newSQLContext()
    import sqlContext.implicits._

    val numAttr = 10
    val attrs = (0 until numAttr).map(i => (1 to 1000).map { case x => x -> i.toDouble }.toMap)
    val g = SmallTestGraph(attrs(0).mapValues(_ => Seq())).result
    val features = attrs.map(attr => AddVertexAttribute.run[Double](g.vs, attr))
    val op = ReduceDimensions(numAttr)
    val result = op(op.features, features).result
    val attr1 = result.attr1.rdd
    val attr2 = result.attr2.rdd
    assert(attr1.count == 1000)
    assert(attr2.count == 1000)
  }
}
