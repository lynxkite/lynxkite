package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.TestGraphOp
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_operations.NoInput

object CreateTestAttributes extends OpFromJson {

  class Output(implicit instance: MetaGraphOperationInstance,
               attrNames: Seq[String], data: Seq[Seq[String]]) extends MagicOutput(instance) {
    val vertices = vertexSet
    val attrs = attrNames.map { a => a -> vertexAttribute[String](vertices, name = Symbol(a)) }.toMap
  }
  def fromJson(j: JsValue) = CreateTestAttributes((j \ "attrNames").as[Seq[String]], (j \ "data").as[Seq[Seq[String]]])

}

case class CreateTestAttributes(val attrNames: Seq[String], data: Seq[Seq[String]])
    extends TypedMetaGraphOp[NoInput, CreateTestAttributes.Output] {

  import CreateTestAttributes._
  override val isHeavy = true
  override val hasCustomSaving = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, attrNames, data)
  override def toJson = Json.obj("attrNames" -> attrNames, "data" -> data)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    val sc = rc.sparkContext

    val attributes = o.attrs.values.map(_.entity)
    val attributesByName = attributes.map(a => a.name -> a).toMap
    val inOrder = attrNames.map(a => attributesByName(Symbol(a)))

    val numberOfRows = data.length
    val partitioner = rc.partitionerForNRows(numberOfRows)
    val ids = (0L until numberOfRows)
    val lines = sc.parallelize(ids.zip(data)).sortUnique(partitioner)

    rc.ioContext.writeAttributes(inOrder, lines)
  }
}

class WriteAttributesTest extends FunSuite with TestGraphOp {

  test("Count attributes separately") {

    val attrNames = Seq("name", "superpower")
    val data = Seq(
      (Seq("Ant-Man", "shrinking")),
      (Seq("Batman", null)),
      (Seq("Captain America", "enchanced strength")),
      (Seq("Deadpool", "super healing")))

    val res = CreateTestAttributes(attrNames, data).result

    val nameCount = res.attrs("name").entity.count.getOrElse(0)
    val superpowerCount = res.attrs("superpower").entity.count.getOrElse(0)
    assert(nameCount == 4 && superpowerCount == 3)
  }

  test("Empty attributes can also be joined") {
    val attrNames = Seq("French military victories")
    val data = Seq(Seq(), Seq(), Seq(), Seq())

    val res = CreateTestAttributes(attrNames, data).result

    val frenchVictories = res.attrs("French military victories").entity.rdd
    val vertices = res.vertices.entity.rdd
    val joined = vertices.leftOuterJoin(frenchVictories)

    assert(frenchVictories.isEmpty())
    assert(vertices.collect.toSeq.sorted == Seq((0, ()), (1, ()), (2, ()), (3, ())))
    assert(joined.collect.toSeq.sorted == Seq((0, ((), None)), (1, ((), None)), (2, ((), None)), (3, ((), None))))
  }
}

