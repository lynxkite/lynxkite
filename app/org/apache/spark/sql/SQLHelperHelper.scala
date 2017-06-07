// Hack to access some private methods.
package org.apache.spark.sql

import org.apache.spark.sql.execution.command.ExplainCommand

object SQLHelperHelper {
  // Invokes an explain command on a data frame, but does not return any results.
  // This is a stripped-down version of DataFrame.explain
  // This method is used when listing the columns necessary for executing an
  // SQL query. It would be a nicer design to parse the resulting query here.
  def explainQuery(df: DataFrame): Unit = {
    val explain = ExplainCommand(df.queryExecution.logical, extended = false)
    df.sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect()
  }

  def newSQLConf = new org.apache.spark.sql.internal.SQLConf()
}
