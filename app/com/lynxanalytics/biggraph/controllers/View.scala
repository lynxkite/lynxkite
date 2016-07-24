// Core classes for representing columnar data using meta graph entities.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import play.api.libs.json

// A ViewRecipe can create a DataFrame representing the view. They are typically JSON request case
// classes. Instead of executing these requests immediately, they can be stored as recipes and
// executed later lazily.
object ViewRecipe {
  def saveAsCheckpoint[T <: ViewRecipe: json.Writes](recipe: T, notes: String = "")(
    implicit manager: MetaGraphManager): String = manager.synchronized {

    val editor = new RootProjectEditor(RootProjectState.emptyState)
    editor.notes = notes
    editor.viewRecipe = recipe
    val cps = manager.checkpointRepo.checkpointState(editor.rootState, prevCheckpoint = "")
    cps.checkpoint.get
  }
}
trait ViewRecipe {
  def createDataFrame(user: serving.User, context: SQLContext)(
    implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame
}

