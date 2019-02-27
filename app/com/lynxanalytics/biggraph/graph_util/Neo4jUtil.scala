package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.neo4j.spark.Neo4j

object Neo4jUtil {

  /**
   * Interface to a cypher query. Provides methods for easing the partitioning and the automatic
   * import of properties for unknown entities
   *
   * @param label Label to identify the Cypher entity (either a node label or a relationship type)
   * @param properties Properties to import
   * @param infer Whether to try to automatically infer data types. If false, all columns will be
   *              imported as Strings. It is recommended to set this to false, as cypher types do
   *              not integrate very well with Spark (Eg. Cypher datetimes not supported at all)
   */
  abstract class CypherQuery(
      neo: Neo4j,
      val label: String,
      properties: Set[String],
      infer: Boolean) {

    /** Build the cypher query to obtain the desired data from Neo4j */
    def buildQuery(): String

    lazy val keys: Set[String] = if (properties.isEmpty) getKeys() else properties

    /**
     * Cypher query to automatically obtain all the properties to import. It should
     * return a comma separated list of property names
     */
    val keysQuery: String

    /** Executes `keysQuery`  and obtains the result */
    // TODO: Probably this would be faster/cleaner using the java driver directly
    def getKeys(): Set[String] = {
      val result = neo
        .cypher(keysQuery)
        .loadDataFrame
        .first()

      if (result.isNullAt(0)) Set.empty else result.getString(0).split(",").toSet
    }

    /** Cypher query to count the existing number of entities (rows) of type `label` */
    val countQuery: String

    /** Executes `countQuery`  and obtains the result*/
    // TODO: Probably this would be faster/cleaner using the java driver directly
    def count(): Int = neo
      .cypher(countQuery)
      .loadDataFrame
      .first()
      .get(0)
      .asInstanceOf[scala.Long].toInt

    /**
     * Transforms a query to support being loaded in batches, and so taking advantage of
     * partitioning. It uses the `SKIP` and `LIMIT` operators from Cypher.
     *
     * Necessary due to not be implemented in the neo4j-spark-connector side. If this clause is
     * not appended to the query, and more than 1 partition is specified, you will end with a
     * dataframe in which all partitions hold the same data. Not only not taking advantage of the
     * partitioning but also using nPartitions times more space.
     *
     * @param query Query to transform
     */
    def batched(query: String): String = query + " SKIP {_skip} LIMIT {_limit}"

    /**
     * If `infer` is false adds a `toString` cast for each property before importing from Neo4j,
     * otherwise tries to import all the properties with their type from Neo4j.
     */
    def strCast(property: String): String = if (!infer) s"toString($property)" else s"$property"
  }

  case class NodeQuery(
      neo: Neo4j,
      override val label: String,
      properties: Set[String],
      infer: Boolean)
    extends cypherQuery(neo, label, properties, infer) {

    override def buildQuery(): String = {
      val keysString = keys.filter(!_.endsWith("$"))
        .map {
          k => s"${strCast("n." + k)} as $k"
        }
        .+(s"${strCast("id(n)")} as id$$")
        .mkString(",")
      batched(s"MATCH (n:$label) RETURN $keysString")
    }

    override val countQuery: String = s"MATCH (:$label) RETURN count(*) as total"

    override val keysQuery: String = s"MATCH (n:$label) WITH keys(n) as k " +
      "RETURN reduce(s = HEAD(k), p IN TAIL(k)| p + ',' + s) LIMIT 1"
  }

  case class RelQuery(
      neo: Neo4j,
      override val label: String,
      properties: Set[String],
      infer: Boolean)
    extends cypherQuery(neo, label, properties, infer) {

    override def buildQuery(): String = {
      // Transform properties starting with 'source_' or 'target_' to 'source.' or
      // 'target.' to allow users to select properties from source or target nodes
      val keysString = keys.filter(!_.endsWith("$"))
        .map { k =>
          if (k.startsWith("source_") || k.startsWith("target_"))
            s"${strCast(k.replace('_', '.'))} as $k"
          else
            s"${strCast("r." + k)} as $k"
        }
        .+(s"${strCast("id(source)")} as source_id$$")
        .+(s"${strCast("id(target)")} as target_id$$")
        .mkString(",")
      batched(s"MATCH (source)-[r:$label]->(target) RETURN $keysString")
    }

    override val countQuery: String = s"MATCH ()-[:$label] ->() RETURN count(*) as total"

    override val keysQuery: String = s"MATCH (a)-[r:$label]->(b) WITH keys(r) as k " +
      "RETURN reduce(s = HEAD(k), p IN TAIL(k)| p + ',' + s) LIMIT 1"
  }

  /**
   * Construct a DataFrame representing the Neo4j entity specified by `node` or `relationship`
   *
   * @param context        SQLcontext
   * @param node           Node label corresponding to an existing label in Neo4j database
   * @param relationship   Relationship type corresponding to an existing type in Neo4j database
   * @param properties     Properties to import from Neo4j
   * @param infer          Whether to try to automatically infer data types
   * @param limit          Limit of rows to load
   * @param numPartitions  Partitions to use for the returned DataFrame
   * @return
   */
  def read(
    context: SQLContext,
    node: String,
    relationship: String,
    properties: Set[String],
    infer: Boolean,
    limit: String,
    numPartitions: Int): DataFrame = {

    assert(node != "" || relationship != "", "Must specify node label or relationship type")
    assert(node == "" || relationship == "", "Cannot define both relationship type and node label")

    val neo = Neo4j(context.sparkContext)

    val query: cypherQuery =
      if (node != "") nodeQuery(neo, node, properties, infer)
      else relQuery(neo, relationship, properties, infer)

    // Get partitioning attributes
    val nRows = if (limit.isEmpty) query.count() else limit.toInt
    val nPart = if (numPartitions <= 0) {
      RuntimeContext.partitionerForNRows(nRows).numPartitions
    } else {
      numPartitions
    }

    val queryString = query.buildQuery()
    log.info(s"Generated cypher query: $queryString")

    val rdd = neo
      .cypher(queryString)
      .partitions(nPart)
      .rows(nRows)
      .loadRowRdd

    // We need this, as neo4j spark connector constructs it's own SQLcontext. Otherwise Datamanager
    // will report that the DataFrame belongs to another context when we try to query it
    context.createDataFrame(rdd, rdd.first().schema)
  }

}
