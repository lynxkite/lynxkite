package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_util.safe_for_interpreter.MagicDate._
class MagicDateTest extends FunSuite {
  private val d = "2017-02-03"
  test("MagicDate works on strings") {
    assert(d.oracle() == s"to_timestamp('$d 00:00:00', 'YYYY-MM-DD HH24:MI:SS')'")
    assert(d.db2() == s"TIMESTAMP('$d 00:00:00')")
    assert(d.teradata() == s"CAST ('$d' AS TIMESTAMP(0) FORMAT 'YYYY-MM-DD')")
  }

  test("MagicDate works with JodaTime arithmetic") {
    import com.github.nscala_time.time.Imports._
    assert((d.plus(1.year)).db2() == "TIMESTAMP('2018-02-03 00:00:00')")
  }

}
