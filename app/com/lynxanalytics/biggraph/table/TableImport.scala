// Code for importing tables into LynxKite.
package com.lynxanalytics.biggraph.table

import org.apache.spark.sql

import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations

object TableImport {
  // Imports the given DataFrame as LynxKite entities wrapped in a Table.
  // The DataManager is used to start the actual import asynchronously.
  def importDataFrameAsync(df: sql.DataFrame)(
    implicit metaManager: MetaGraphManager, dataManager: DataManager): controllers.Table = {
    import Scripting._
    val importMetaResult = graph_operations.ImportDataFrame(df).result
    val columnEntities = importMetaResult.columns.mapValues(_.entity)
    for (attr <- columnEntities.values) dataManager.getFuture(attr)
    controllers.RawTable(importMetaResult.ids, columnEntities)
  }
}
