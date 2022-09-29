// Turns a VertexSet and its Attributes into a Table.
package com.lynxanalytics.lynxkite.graph_operations

import org.apache.spark
import org.apache.spark.sql.types
import scala.reflect.runtime.universe._

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.SQLHelper

object AttributesToTable extends OpFromJson {
  class Input(attributes: Iterable[(String, TypeTag[_])]) extends MagicInputSignature {
    val vs = vertexSet
    private def va[T: TypeTag](name: String) = vertexAttribute[T](vs, Symbol(name))
    val attrs: Iterable[VertexAttributeTemplate[_]] =
      attributes.map { case (name, tt) => va(name)(tt) }
  }
  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance)
      extends MagicOutput(instance) {
    val t = table(schema)
  }

  import Scripting._
  // Ask the type system to trust us that this attribute matches the template type.
  private def build[T, IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
      builder: InstanceBuilder[IS, OMDS],
      template: Input#VertexAttributeTemplate[_],
      attribute: Attribute[T]) =
    builder(template.asInstanceOf[Input#VertexAttributeTemplate[T]], attribute)

  def run(vs: VertexSet, attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager): Table = {
    val op = AttributesToTable(SQLHelper.dataFrameSchema(attributes))
    op.attrs.zip(attributes).foldLeft(op(op.vs, vs)) {
      case (builder, (template, (name, attribute))) =>
        build(builder, template, attribute)
    }.result.t
  }

  def run(attributes: Iterable[(String, Attribute[_])])(implicit m: MetaGraphManager): Table = {
    assert(attributes.nonEmpty, "Cannot infer vertexSet without attributes")
    run(attributes.head._2.vertexSet, attributes)
  }

  def fromJson(j: JsValue) = {
    AttributesToTable(
      types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType])
  }
}
import AttributesToTable._
case class AttributesToTable(schema: types.StructType) extends SparkOperation[Input, Output] {
  override val isHeavy = false // A goal here is quick access to a project as a DataFrame.
  @transient override lazy val inputs = {
    new Input(schema.map {
      field => field.name -> SQLHelper.typeTagFromDataType(field.dataType)
    })
  }

  override def toJson = Json.obj("schema" -> schema.prettyJson)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(schema)(instance)

  def execute(
      inputDatas: DataSet,
      o: Output,
      output: OutputBuilder,
      rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val columnRDDs: Map[String, AttributeRDD[_]] = inputs.attrs.map { attr =>
      schema(attr.name.name).dataType match {
        case _: types.StringType =>
          // Since StringType is the default attribute type the data may need to be converted.
          attr.name.name -> attr.rdd.mapValues(_.toString)
        case _: types.ArrayType if attr.data.entity.typeTag.tpe <:< typeOf[Set[_]] =>
          // Sets are represented by Spark as arrays. (Introduced in SPARK-21204.)
          // runtimeSafeCast does not know about covariance so we do an unsafe cast.
          attr.name.name -> attr.data.asInstanceOf[AttributeData[Set[_]]].rdd.mapValues(_.toSeq)
        case _ => attr.name.name -> attr.rdd
      }
    }.toMap
    val df = new RDDRelation(schema, inputs.vs.rdd, columnRDDs, rc.sqlContext).toDF
    output(o.t, df)
  }
}

class RDDRelation(
    val schema: types.StructType,
    vertexSetRDD: VertexSetRDD,
    columnRDDs: Map[String, AttributeRDD[_]],
    val sqlContext: spark.sql.SQLContext)
    extends spark.sql.sources.BaseRelation
    with spark.sql.sources.TableScan
    with spark.sql.sources.PrunedScan {
  def toDF = sqlContext.baseRelationToDataFrame(this)

  // TableScan
  def buildScan(): spark.rdd.RDD[spark.sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): spark.rdd.RDD[spark.sql.Row] = {
    val rdds = requiredColumns.toSeq.map(name => columnRDDs(name))
    val emptyRows = vertexSetRDD.mapValues(_ => Seq[Any]())
    val seqRows = rdds.foldLeft(emptyRows) { (seqs, rdd) =>
      seqs.sortedLeftOuterJoin(rdd).mapValues { case (seq, opt) => seq :+ opt.getOrElse(null) }
    }
    seqRows.values.map(spark.sql.Row.fromSeq(_))
  }
}
