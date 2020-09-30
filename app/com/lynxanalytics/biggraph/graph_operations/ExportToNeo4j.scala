package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import org.apache.spark
import org.apache.spark.sql.SaveMode

object ExportAttributesToNeo4j extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }
  def fromJson(j: JsValue) = ExportAttributesToNeo4j(
    (j \ "url").as[String], (j \ "username").as[String], (j \ "password").as[String],
    (j \ "labels").as[String], (j \ "keys").as[Seq[String]], (j \ "version").as[Long],
    (j \ "nodesOrRelationships").as[String])
}

case class ExportAttributesToNeo4j(
    url: String, username: String, password: String, labels: String, keys: Seq[String], version: Long, nodesOrRelationships: String)
  extends SparkOperation[ExportAttributesToNeo4j.Input, ExportAttributesToNeo4j.Output] {
  @transient override lazy val inputs = new ExportAttributesToNeo4j.Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new ExportAttributesToNeo4j.Output()(instance, inputs)
  override def toJson = Json.obj(
    "url" -> url, "username" -> username, "password" -> password, "labels" -> labels,
    "keys" -> keys, "version" -> version, "nodesOrRelationships" -> nodesOrRelationships)
  def execute(
    inputDatas: DataSet,
    o: ExportAttributesToNeo4j.Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    implicit val sd = rc.sparkDomain
    val df = inputs.t.df
    val keyMatch = keys.map(k => s"`$k`: event.`$k`").mkString(", ")
    val query = s"MERGE (n$labels {$keyMatch}) SET n += event"
    df.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("authentication.type", "basic")
      .option("authentication.basic.username", username)
      .option("authentication.basic.password", password)
      .option("url", url)
      .option("query", query)
      .save()
    val exportResult = "Export done."
    output(o.exportResult, exportResult)
  }
}
