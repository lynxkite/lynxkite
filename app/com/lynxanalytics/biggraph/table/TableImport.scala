// Code for importing tables into LynxKite.
package com.lynxanalytics.biggraph.table

import org.apache.spark.sql

import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations

object TableImport {
  // This is a blocking operation that imports the given dataframe as
  // LynxKite entities. The imported entities are returned as a Table.
  def importDataFrame(df: sql.DataFrame)(
    implicit metaManager: MetaGraphManager, dataManager: DataManager): controllers.Table = {
    import Scripting._
    val importMetaResult = graph_operations.ImportDataFrame(df).result
    val columnEntities = importMetaResult.columns.mapValues(_.entity)

    // Enfore the actual import.
    columnEntities.values.foreach(attr => dataManager.get(attr))

    controllers.RawTable(importMetaResult.ids, columnEntities)
  }
}
