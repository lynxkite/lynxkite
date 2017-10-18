// Usage:
//   1) start a spark-shell
//   2) :load path/to/this/file
//   3) create a map (name -> (pathOfExpected, pathOfActual)) with the files to compare 
//   4) val problems = compareAll(map)
//   5) inspect problems


import org.apache.spark.sql.DataFrame


case class Problem(problemType: String, details: String = "")

def frameHashCode(dataFrame: DataFrame): Int =
  dataFrame.map(_.hashCode).reduce(_+_)

def compareDataFrames(df1: DataFrame, df2: DataFrame): Option[Problem] = {
  val df1Schema = df1.schema.toString.toLowerCase
  val df2Schema = df2.schema.toString.toLowerCase
  if (df1Schema != df2Schema) {
    Some(Problem(problemType = "schema mismatch",
      details = s"Expected:\n$df1Schema\n\nvs.\n\nActual:\n$df2Schema"))
  } else if (df1.count != df2.count) {
    Some(Problem(problemType = "count mismatch",
      details = s"Expected: ${df1.count} vs. Actual: ${df2.count}"))
  } else if (df1.count == 0) {
    Some(Problem(problemType = "both emtpy"))
  } else if (frameHashCode(df1) != frameHashCode(df2)) {
    Some(Problem(problemType = "hash mismatch"))
  } else {
    None
  }
}

// spark is the spark context in the spark-shell, it should be available here
def readParquet(path: String) = spark.read.parquet(path)

def compareAll(whatToWhat: Map[String, (String, String)]): Map[String, Option[Problem]] =
  whatToWhat.mapValues {
    case (path1, path2) =>
      try {
        val df1 = readParquet(path1)
        val df2 = readParquet(path2)
        compareDataFrames(df1, df2)
      } catch {
        case ex: Exception =>
          Some(Problem(problemType = "exception thrown", details = ex.getMessage))
      }
  }
