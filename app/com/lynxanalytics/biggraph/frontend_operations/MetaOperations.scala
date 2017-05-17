// Frontend operations that do not represent actual operations.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers._

class MetaOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import OperationParams._

  def register(
    id: String,
    category: Category)(factory: Context => Operation): Unit = {
    registerOp(id, category, List(), List(), factory)
  }

  // Categories
  val OtherBoxes = Category("Other boxes", "black", icon = "kraken")
  val AnchorBox = Category("Anchor box", "black", icon = "kraken", visible = false)

  register("Add comment", OtherBoxes)(new DecoratorOperation(_) {
    params += Code("comment", "Comment", language = "plain_text")
  })

  register("Anchor", AnchorBox)(new DecoratorOperation(_) {
    params += Code("description", "Description", language = "plain_text")
    params += ParametersParam("parameters", "Parameters")
  })

  registerOp(
    "Input box", OtherBoxes,
    List(), List("input"),
    new MinimalOperation(_) {
      params += Param("name", "Name")
    })

  registerOp(
    "Output box", OtherBoxes,
    List("output"), List(),
    new MinimalOperation(_) {
      params += Param("name", "Name")
    })
}
