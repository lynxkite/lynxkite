package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Attribute
import com.lynxanalytics.biggraph.graph_operations.ExecuteSQL

object SQLTestCases {
  val allColumns = Set("age", "gender", "income", "location")
  val list: Seq[(String, Map[String, Set[String]])] = Seq(
    ("select age as notage from one", Map("one" -> Set("age"))),
    ("select count(gender) from one", Map("one" -> Set("gender"))),
    ("select count(*) from one", Map("one" -> Set("age"))),
    ("select sin((sqrt(age) * 0.3) + 1) from one", Map("one" -> Set("age"))),
    ("select count(*) from one cross join two", Map("one" -> Set("age"), "two" -> Set("age"))),
    ("select * from one", Map("one" -> Set("age", "gender", "income", "location"))),
    ("select age from one where gender < 3", Map("one" -> Set("age", "gender"))),
    ("select o.age, t.income from one o cross join two t", Map("one" -> Set("age"), "two" -> Set("income"))),
    ("select o.age, t.income from one o cross join one t", Map("one" -> Set("age", "income"))),
    ("select age from (select * from one)", Map("one" -> allColumns)),
    ("select age from (select age, gender from one)", Map("one" -> Set("age", "gender"))),
    ("select 1 from (select gender from one)", Map("one" -> Set("gender"))),
    ("select age from (select * from one) where gender=11", Map("one" -> allColumns)),
    (
      "select o.age, two.income from one o inner join two on o.gender=two.gender where o.age=1",
      Map("one" -> Set("age", "gender"), "two" -> Set("gender", "income"))),
    (
      "select age from (select gender + gender, age from (select gender, age from one))",
      Map("one" -> Set("age", "gender"))),
    (
      "select sum(cnt) as cnt from (select count(age) as cnt from one union all select count(income) as cnt from two)",
      Map("one" -> Set("age"), "two" -> Set("income"))),
    (
      "select age from one where income in (select income from two)",
      Map("one" -> Set("age", "income"), "two" -> Set("income"))),
  )
}

class ProtoTableTest extends BigGraphControllerTestBase {
  private def attr(name: String): (String, Attribute[_]) =
    (name, Attribute[String](null, Symbol(name), null))

  val attrs = SQLTestCases.allColumns.map(attr)
  val one = ProtoTable(null, attrs)
  val two = ProtoTable(null, attrs)
  val protoTables = Map("one" -> one, "two" -> two)

  private def compareProto(expected: Map[String, Set[String]], actual: Map[String, ProtoTable]) = {
    assert(expected == actual.mapValues(_.schema.map(_.name).toSet))
  }

  SQLTestCases.list.foreach {
    case (query, expected) =>
      test(query) {
        val plan = ExecuteSQL.getLogicalPlan(query, protoTables)
        val minimizedTables = ProtoTable.minimize(plan, protoTables)
        compareProto(expected, minimizedTables)
      }
  }
}
