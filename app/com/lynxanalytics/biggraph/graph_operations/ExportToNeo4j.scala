// Backend operations for Neo4j export.
package com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import org.apache.spark

object ExportAttributesToNeo4j extends OpFromJson {
  class Input extends MagicInputSignature {
    val t = table
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }
  def fromJson(j: JsValue) = ExportAttributesToNeo4j(
    (j \ "url").as[String], (j \ "username").as[String], (j \ "password").as[String],
    (j \ "labels").as[String], (j \ "keys").as[Seq[String]], (j \ "version").as[Long],
    (j \ "nodesOrRelationships").as[String])
}

// Makes it easy to send a DataFrame to a specified Neo4j instance.
case class Neo4jConnectionParameters(url: String, username: String, password: String) {
  def send(df: spark.sql.DataFrame, query: String) {
    df.write
      .format("org.neo4j.spark.DataSource")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", username)
      .option("authentication.basic.password", password)
      .option("url", url)
      .option("query", query)
      .mode(spark.sql.SaveMode.Append)
      .save()
  }
}

case class ExportAttributesToNeo4j(
    url: String, username: String, password: String, labels: String, keys: Seq[String],
    version: Long, nodesOrRelationships: String)
  extends SparkOperation[ExportAttributesToNeo4j.Input, ExportAttributesToNeo4j.Output] {
  val neo = Neo4jConnectionParameters(url, username, password)
  @transient override lazy val inputs = new ExportAttributesToNeo4j.Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new ExportAttributesToNeo4j.Output()(instance)
  override def toJson = Json.obj(
    "url" -> url, "username" -> username, "password" -> password, "labels" -> labels,
    "keys" -> keys, "version" -> version, "nodesOrRelationships" -> nodesOrRelationships)
  def execute(
    inputDatas: DataSet,
    o: ExportAttributesToNeo4j.Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    // Drop null keys.
    val df = keys.foldLeft(inputs.t.df)((df, key) => df.filter(df(key).isNotNull))
    val keyMatch = keys.map(k => s"`$k`: event.`$k`").mkString(", ")
    val query = nodesOrRelationships match {
      case "nodes" => s"MATCH (n$labels {$keyMatch}) SET n += event"
      case "relationships" => s"MATCH ()-[r$labels {$keyMatch}]-() SET r += event"
    }
    neo.send(df, query)
    val exportResult = "Export done."
    output(o.exportResult, exportResult)
  }
}

object ExportGraphToNeo4j extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = table
    val es = table
  }
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val exportResult = scalar[String]
  }
  def fromJson(j: JsValue) = ExportGraphToNeo4j(
    (j \ "url").as[String], (j \ "username").as[String], (j \ "password").as[String],
    (j \ "nodeLabelsColumn").as[String], (j \ "relationshipTypeColumn").as[String],
    (j \ "version").as[Long])
  val VID = "!LynxKite ID"
  val SRCDST = "!LynxKite endpoint IDs"
  val SRCID = "!Source LynxKite ID"
  val DSTID = "!Destination LynxKite ID"
}

case class ExportGraphToNeo4j(
    url: String, username: String, password: String, nodeLabelsColumn: String,
    relationshipTypeColumn: String, version: Long)
  extends SparkOperation[ExportGraphToNeo4j.Input, ExportGraphToNeo4j.Output] {
  import ExportGraphToNeo4j._
  val neo = Neo4jConnectionParameters(url, username, password)
  @transient override lazy val inputs = new ExportGraphToNeo4j.Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new ExportGraphToNeo4j.Output()(instance)
  override def toJson = Json.obj(
    "url" -> url, "username" -> username, "password" -> password,
    "nodeLabelsColumn" -> nodeLabelsColumn, "relationshipTypeColumn" -> relationshipTypeColumn,
    "version" -> version)
  def execute(
    inputDatas: DataSet,
    o: ExportGraphToNeo4j.Output,
    output: OutputBuilder,
    rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val F = spark.sql.functions
    // Prefix the internal IDs with the timestamp so different exports don't collide.
    // Also save the timestamp so the created entities can be easily cleaned up.
    val timestamp = F.lit(Timestamp.human)
    val vs = inputs.vs.df
      .withColumn(VID, F.concat(timestamp, F.lit(" "), F.col(VID)))
      .withColumn("!LynxKite export timestamp", timestamp)
    val es = inputs.es.df
      .withColumn(SRCID, F.concat(timestamp, F.lit(" "), F.col(SRCID)))
      .withColumn(DSTID, F.concat(timestamp, F.lit(" "), F.col(DSTID)))
      .withColumn("!LynxKite export timestamp", timestamp)

    if (nodeLabelsColumn.isEmpty) {
      neo.send(vs, s"""
        CREATE (n)
        SET n += event
      """)
    } else {
      neo.send(vs, s"""
        CALL apoc.create.node(split(event.`$nodeLabelsColumn`, ','), event) YIELD node
        RETURN 1
      """)
    }
    if (relationshipTypeColumn.isEmpty) {
      neo.send(es, s"""
      MATCH (src {`$VID`: event.`$SRCID`}), (dst {`$VID`: event.`$DSTID`})
      CREATE (src)-[r:EDGE]->(dst)
      SET r += event
    """)
    } else {
      neo.send(es, s"""
      MATCH (src {`$VID`: event.`$SRCID`}), (dst {`$VID`: event.`$DSTID`})
      CALL apoc.create.relationship(src, event.`$relationshipTypeColumn`, event, dst) YIELD rel
      RETURN 1
    """)
    }
    val exportResult = "Export done."
    output(o.exportResult, exportResult)
  }
}
