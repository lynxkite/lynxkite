// A somewhat more complex graph with all sorts of attributes. Used for testing.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object EnhancedExampleGraph extends OpFromJson {
  class Input extends MagicInputSignature {
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val (vertices, edges) = graph
    val name = vertexAttribute[String](vertices)
    val age = vertexAttribute[Double](vertices)
    val gender = vertexAttribute[String](vertices)
    val income = vertexAttribute[Double](vertices) // Partially defined.
    val location = vertexAttribute[(Double, Double)](vertices)
    val comment = edgeAttribute[String](edges)
    val weight = edgeAttribute[Double](edges)
    val greeting = scalar[String]
    // For wholesale attribute access.
    val edgeAttributes = Map("comment" -> comment, "weight" -> weight)
    val vertexAttributes = Map(
      "name" -> name,
      "age" -> age,
      "gender" -> gender,
      "income" -> income,
      "location" -> location)
  }
  def fromJson(j: JsValue) = EnhancedExampleGraph()

  val Adam = 0L
  val Eve = 1L
  val Bob = 2L
  val Joe = 3L
  val Cat = 4L
  val Fish = 5L
  val Mouse = 6L
  val Wanda = 7L

  val eAdamEve = 0L
  val eEveAdam = 1L
  val eBobAdam = 2L
  val eBobEve = 3L
  val eBobBob = 4L
  val eBobCat = 5L
  val eBobFish1 = 6L
  val eBobFish2 = 7L
  val eBobFish3 = 8L
  val eFishBob1 = 9L
  val eFishBob2 = 10L
  val eFishCat1 = 11L
  val eFishCat2 = 12L
  val eFishCat3 = 13L
  val eCatFish = 14L
  val eCatBob1 = 15L
  val eCatBob2 = 16L
  val eMouseCat = 17L
  val eFishWanda = 18L

  val firstEdge = eAdamEve
  val lastEdge = eFishWanda

}
import EnhancedExampleGraph._
case class EnhancedExampleGraph() extends TypedMetaGraphOp[Input, Output] {
  @transient var executionCounter = 0

  override val isHeavy = true
  @transient override lazy val inputs = new Input()

  def outputMeta(instance: MetaGraphOperationInstance) =
    new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: EnhancedExampleGraph.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    executionCounter += 1

    val sc = rc.sparkContext
    val partitioner = rc.onePartitionPartitioner

    output(
      o.vertices,
      sc.parallelize((Adam to Wanda).map((_, ())))
        .sortUnique(partitioner))
    output(
      o.edges,
      sc.parallelize(Seq(
        (eAdamEve, Edge(Adam, Eve)),
        (eEveAdam, Edge(Eve, Adam)),
        (eBobAdam, Edge(Bob, Adam)),
        (eBobEve, Edge(Bob, Eve)),
        (eBobBob, Edge(Bob, Bob)),
        (eBobCat, Edge(Bob, Cat)),
        (eBobFish1, Edge(Bob, Fish)),
        (eBobFish2, Edge(Bob, Fish)),
        (eBobFish3, Edge(Bob, Fish)),
        (eFishBob1, Edge(Fish, Bob)),
        (eFishBob2, Edge(Fish, Bob)),
        (eFishCat1, Edge(Fish, Cat)),
        (eFishCat2, Edge(Fish, Cat)),
        (eFishCat3, Edge(Fish, Cat)),
        (eCatFish, Edge(Cat, Fish)),
        (eCatBob1, Edge(Cat, Bob)),
        (eCatBob2, Edge(Cat, Bob)),
        (eMouseCat, Edge(Mouse, Cat)),
        (eFishWanda, Edge(Fish, Wanda))))
        .sortUnique(partitioner))
    output(o.name, sc.parallelize(Seq(
      (Adam, "Adam"),
      (Eve, "Eve"),
      (Bob, "Bob"),
      (Joe, "Isolated Joe"),
      (Cat, "Cat"),
      (Fish, "Fish"),
      (Mouse, "Mouse"),
      (Wanda, "Wanda"))).sortUnique(partitioner))
    output(o.age, sc.parallelize(Seq(
      (Adam, 20.3),
      (Eve, 18.2),
      (Bob, 50.3),
      (Joe, 2.0),
      (Cat, 12.0),
      (Fish, 5.0),
      (Mouse, 41.0),
      (Wanda, 26.1))).sortUnique(partitioner))
    output(o.gender, sc.parallelize(Seq(
      (Adam, "Male"),
      (Eve, "Female"),
      (Bob, "Male"),
      (Joe, "Male"),
      (Cat, "Female"),
      (Fish, "Female"),
      (Mouse, "Male"),
      (Wanda, "Female"))).sortUnique(partitioner))
    output(o.income, sc.parallelize(Seq(
      (Adam, 1000.0),
      (Joe, 2000.0))).sortUnique(partitioner))
    output(o.location, sc.parallelize(Seq(
      (Adam, (40.71448, -74.00598)), // New York
      (Eve, (47.5269674, 19.0323968)), // Budapest
      (Bob, (1.352083, 103.819836)), // Singapore
      (Joe, (-33.8674869, 151.2069902)), // Sydney
      (Cat, (1.352083, 103.819836)), // Singapore
      (Fish, (1.352083, 103.819836)), // Singapore
      (Mouse, (1.352083, 103.819836)), // Singapore
      (Wanda, (3.1412, 101.68653)) // Kuala Lumpur
    )).sortUnique(partitioner))
    output(o.comment, sc.parallelize(Seq(
      (eAdamEve, "Adam loves Eve"),
      (eEveAdam, "Eve loves Adam"),
      (eBobAdam, "Bob envies Adam"),
      (eBobEve, "Bob loves Eve"),
      (eBobBob, "Bob hates himself"),
      (eBobCat, "Bob doesn't see the cat"),
      (eBobFish1, "Bob owns the fish"),
      (eBobFish2, "Bob feeds the fish"),
      (eBobFish3, "Bob complains to the fish"),
      (eFishBob1, "The fish understands Bob"),
      (eFishBob2, "The fish helps Bob"),
      (eFishCat1, "The fish summons the cat"),
      (eFishCat2, "The fish mesmerizes the cat"),
      (eFishCat3, "The fish controls the cat"),
      (eCatFish, "The cat obeys the fish"),
      (eCatBob1, "The cat distracts Bob's attention"),
      (eCatBob2, "The cat steals Bob's phone"),
      (eMouseCat, "The mouse sees the cat"),
      (eFishWanda, "The fish calls Wanda"))).sortUnique(partitioner))
    output(o.weight, sc.parallelize((firstEdge to lastEdge).map { x => (x, x.toDouble) })
      .sortUnique(partitioner))

    output(o.greeting, "Hello world! 😀 ")
  }
}
