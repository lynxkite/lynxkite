package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.TestGraphOp
import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_operations.NoInput
import com.lynxanalytics.biggraph.graph_api._

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
  override val hasCustomSaving = true // Single-pass import.
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
    val ids = (0L until numberOfRows).toSeq
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

  test("Writer is created even if the partition is empty") {
    val attrNames = Seq("French millitary victories")
    val data = Seq(Seq(), Seq(), Seq(), Seq())

    val res = CreateTestAttributes(attrNames, data).result

    val numberOfPartitions = res.attrs("French millitary victories").entity.rdd.partitions.length
    assert(numberOfPartitions == 1)
  }
}

