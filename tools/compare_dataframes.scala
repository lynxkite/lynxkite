// Usage:
//   1) start a spark-shell
//   2) :load path/to/this/file
//   3) create a map (name -> (pathOfExpected, pathOfActual)) with the files to compare
//   4) val problems = compareAll(map)
//   5) inspect problems

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions
import util.Try

def isClose(a: Double, b: Double, rtol: Double = 1e-5, atol: Double = 1e-8): Boolean =
  Math.abs(a - b) < Math.max(atol, Math.abs(a) * rtol)

def doubleColSum(df: DataFrame, colName: String): Double =
  df.select(colName).as[Double].reduce(_ + _)

def columnHash(df: DataFrame, colName: String): Int =
  df.select(hash(df(colName))).as[Int].reduce(_ + _)

def compareDataFrames(df1: DataFrame, df2: DataFrame): Unit = {
  val df1Schema = df1.schema
  val df2Schema = df2.schema
  assert(df1Schema.toString.toLowerCase == df2Schema.toString.toLowerCase, "schema mismatch")
  assert(df1.count == df2.count, "count mismatch")
  assert(df1.count != 0, "both emtpy")
  df1Schema.foreach { col =>
    val colName = col.name
    if (col.dataType == DoubleType) {
      val df1Sum = doubleColSum(df1, colName)
      val df2Sum = doubleColSum(df2, colName)
      assert(isClose(df1Sum, df2Sum), s"$colName sum mismatch")
    } else {
      assert(columnHash(df1, colName) == columnHash(df2, colName), s"$colName hash mismatch")
    }
  }
}

// spark is the spark context in the spark-shell, it should be available here
def readParquet(path: String) = spark.read.parquet(path)

def compareAll(whatToWhat: Map[String, (String, String)]): Map[String, Try[Unit]] =
  whatToWhat.map {
    case (name, (path1, path2)) =>
      name -> Try {
        val df1 = readParquet(path1)
        val df2 = readParquet(path2)
        compareDataFrames(df1, df2)
      }
  }
