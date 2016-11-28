// Hack to access some private methods.
package org.apache.spark.sql

import org.apache.spark.sql.execution.command.ExplainCommand

object SQLHelperHelper {
  // Invokes an explain command on a data frame, but does not return any results.
  // This is a stripped-down version of DataFrame.explain
  def explainQuery(df: DataFrame) {
    val explain = ExplainCommand(df.queryExecution.logical, extended = false)
    df.sqlContext.executePlan(explain).executedPlan.executeCollect()
    // This method is used when listing the columns necessary for executing an
    // SQL query. It would be a nicer design to parse the resulting query here.
    // As of Spark 1.6, the result of the above command is a list of strings,
    // which is generated inside ExplainCommand.execute.
  }
}
