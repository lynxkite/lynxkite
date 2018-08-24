package com.lynxanalytics.biggraph.graph_util.safe_for_interpreter

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.language.implicitConversions

object MagicDate {
  val ymd = "yyyy-MM-dd"
  val hms = "HH:mm:ss"
  val fullDate = s"$ymd $hms"

  implicit def stringToDate(original: String): DateTime = {
    original.toDateTime(fullDate)
  }
  implicit class MagicString(date: DateTime) {
    def format(str: String): String = {
      DateTimeFormat.forPattern(str).print(date)
    }

    def oracle(): String = {
      val dt = format(fullDate)
      s"to_timestamp('$dt', 'YYYY-MM-DD HH24:MI:SS')'"
    }

    def teradata(): String = {
      val dt = format(s"$ymd")
      s"CAST ('$dt' AS TIMESTAMP(0) FORMAT 'YYYY-MM-DD')"
    }

    def db2(): String = {
      val dt = format(fullDate)
      s"TIMESTAMP('$dt')"
    }
  }
  implicit class MagicDatetime(datetime: String) extends MagicString(datetime.toDateTime(fullDate))
}
