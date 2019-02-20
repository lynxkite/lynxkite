package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.graph_util.Neo4jUtil.{ cypherQuery, nodeQuery, relQuery }
import org.neo4j.harness.{ ServerControls, TestServerBuilders }
import org.neo4j.spark.Neo4j
import org.scalatest.{ BeforeAndAfterAll, FunSuite }

class Neo4jUtilTest extends FunSuite with BeforeAndAfterAll with TestSparkContext {

  val FIXTURE: String =
    """
      UNWIND range(1,100) as pid
      CREATE (p:Person {id:pid, name:"p-"+pid, alive: pid < 3, height: pid*1.5})
      WITH collect(p) as people
      UNWIND people as p1
      UNWIND range(1,2) as friend
      WITH p1, people[(id(p1) + friend) % size(people)] as p2
      CREATE (p1)-[:KNOWS {spy: p1.id < 30}]->(p2)
    """

  private var server: ServerControls = _

  override protected def beforeAll(): Unit = {
    server = TestServerBuilders.newInProcessBuilder
      .withConfig("dbms.security.auth_enabled", "false")
      .withConfig("dbms.connector.bolt.listen_address", "localhost:7687")
      .withFixture(FIXTURE)
      .newServer
  }

  override protected def afterAll(): Unit = server.close()

  test("CypherQuery getKeys: Node with properties") {
    val neo = Neo4j(sparkContext)
    server.graph.execute("CREATE (p: PROPS {id:1, p1:'v1', p2: false})")
    val query: cypherQuery = nodeQuery(neo, "PROPS", Set.empty, false)

    assert(query.getKeys() == Set("id", "p1", "p2"))
  }

  test("CypherQuery getKeys: Node with no properties") {
    val neo = Neo4j(sparkContext)
    server.graph.execute("CREATE (p: NOPROPS)")
    val query: cypherQuery = nodeQuery(neo, "NOPROPS", Set.empty, false)

    assert(query.keys == Set.empty)
  }

  test("CypherQuery getKeys: Relationship with properties") {
    val neo = Neo4j(sparkContext)
    server.graph.execute("CREATE ()-[:PROPS {id:1}]->()")
    val query: cypherQuery = relQuery(neo, "PROPS", Set.empty, false)

    assert(query.getKeys() == Set("id"))
  }

  test("CypherQuery getKeys: Relationship with no properties") {
    val neo = Neo4j(sparkContext)
    server.graph.execute("CREATE ()-[:NOPROPS]->()")
    val query: cypherQuery = relQuery(neo, "NOPROPS", Set.empty, false)

    assert(query.getKeys() == Set.empty)
  }

  test("CypherQuery count: Node ") {
    val neo = Neo4j(sparkContext)
    val query: cypherQuery = nodeQuery(neo, "Person", Set.empty, false)

    assert(query.count() == 100)
  }

  test("CypherQuery count: Relationship ") {
    val neo = Neo4j(sparkContext)
    val query: cypherQuery = relQuery(neo, "KNOWS", Set.empty, false)

    assert(query.count() == 200)
  }

  test("read: Correct partitioning for Node") {
    val df = Neo4jUtil.read(sparkSession.sqlContext, node = "Person", relationship = "",
      Set.empty, infer = false, limit = "", numPartitions = 10)

    assert(df.rdd.getNumPartitions == 10)
    assert(df.rdd
      .mapPartitions(iter => Array(iter.size).iterator, true)
      .collect()
      .forall(_ == 10))
    assert(df.select("id").distinct().count() == 100)
  }

  test("read: Correct partitioning for Relationship") {
    val df = Neo4jUtil.read(sparkSession.sqlContext, node = "", relationship = "KNOWS",
      Set.empty, infer = false, limit = "", numPartitions = 10)

    assert(df.rdd.getNumPartitions == 10)
    assert(df.rdd
      .mapPartitions(iter => Array(iter.size).iterator, true)
      .collect()
      .forall(_ == 20))
    assert(df.select("source_id$").distinct().count() == 100)
  }

}
