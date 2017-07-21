// Turns Scalars into a Table.
package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.apache.spark.sql.{ Row, types }

import scala.reflect.runtime.universe._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.protection.Limitations
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import org.apache.spark.rdd.RDD

object ScalarsToTable extends OpFromJson {
  class Input(scalars: Iterable[(String, TypeTag[_])]) extends MagicInputSignature {
    private def sa[T: TypeTag](name: String) = scalar[T](Symbol(name))
    val sclrs: Iterable[ScalarTemplate[_]] =
      scalars.map { case (name, tt) => sa(name)(tt) }
  }
  class Output(schema: types.StructType)(
      implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val t = table(schema)
  }

  import Scripting._
  // Ask the type system to trust us that this attribute matches the template type.
  private def build[T, IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    builder: InstanceBuilder[IS, OMDS],
    template: Input#ScalarTemplate[_],
    scalar: Scalar[T]) =
    builder(template.asInstanceOf[Input#ScalarTemplate[T]], scalar)

  def run(scalars: Iterable[(String, Scalar[_])])(implicit m: MetaGraphManager): Table = {
    val op: ScalarsToTable = ScalarsToTable(SQLHelper.dataFrameSchemaScalar(scalars))
    op.sclrs.zip(scalars).foldLeft(InstanceBuilder(op)) {
      case (builder, (template, (name, scalar))) => build(builder, template, scalar)
    }.result.t
  }

  def fromJson(j: JsValue) = {
    ScalarsToTable(
      types.DataType.fromJson((j \ "schema").as[String]).asInstanceOf[types.StructType])
  }
}
import ScalarsToTable._
case class ScalarsToTable(schema: types.StructType) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = false // A goal here is quick access to a project as a DataFrame.
  @transient override lazy val inputs = {
    new Input(schema.map {
      field => field.name -> SQLHelper.typeTagFromDataType(field.dataType)
    })
  }

  override def toJson = Json.obj("schema" -> schema.prettyJson)

  def outputMeta(instance: MetaGraphOperationInstance) = new Output(schema)(instance)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val df = new SRDDRelation(schema, inputDatas, rc.sqlContext).toDF
    output(o.t, df)
  }
}

class SRDDRelation(
  val schema: types.StructType,
  val inputDatas: DataSet,
  val sqlContext: spark.sql.SQLContext)
    extends spark.sql.sources.BaseRelation
    with spark.sql.sources.TableScan
    with spark.sql.sources.PrunedScan {
  def toDF = sqlContext.baseRelationToDataFrame(this)

  // TableScan
  def buildScan(): spark.rdd.RDD[spark.sql.Row] = buildScan(schema.fieldNames)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): spark.rdd.RDD[spark.sql.Row] = {
    val usedColumns = inputDatas.scalars
    val rdds = requiredColumns.toSeq.map(name => usedColumns(Symbol(name)).value)
    val row = Row.fromSeq(rdds)

    val rdd: RDD[Any] = sqlContext.sparkContext.parallelize(Seq(1)) // creates one row
    val rowRdd = rdd.map(f => row) // fills up the row
    rowRdd
  }
}
