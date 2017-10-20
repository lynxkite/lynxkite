// Usage:
//   1) start a spark-shell
//   2) :load path/to/this/file
//   3) create a map (name -> (pathOfExpected, pathOfActual)) with the files to compare
//   4) val problems = compareAll(map)
//   5) inspect problems

import org.apache.spark.sql.DataFrame
import util.{ Try, Success, Failure }

def frameHashCode(dataFrame: DataFrame): Int =
  dataFrame.map(_.hashCode).reduce(_ + _)

def compareDataFrames(df1: DataFrame, df2: DataFrame): Unit = {
  val df1Schema = df1.schema.toString.toLowerCase
  val df2Schema = df2.schema.toString.toLowerCase
  assert(df1Schema == df2Schema, "schema mismatch")
  assert(df1.count == df2.count, "count mismatch")
  assert(df1.count != 0, "both emtpy")
  assert(frameHashCode(df1) == frameHashCode(df2), "hash mismatch")
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
