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
    category: Category,
    icon: String = "black_medium_square")(factory: Context => Operation): Unit = {
    registerOp(id, icon, category, List(), List(), factory)
  }

  // Categories
  val OtherBoxes = Category("Other boxes", "yellow", icon = "kraken")
  val AnchorBox = Category("Anchor box", "yellow", icon = "kraken", visible = false)

  register("Comment", OtherBoxes)(new DecoratorOperation(_) {
    params += Code("comment", "Comment", language = "plain_text")
  })

  register("Anchor", AnchorBox, "anchor")(new DecoratorOperation(_) {
    params += Code("description", "Description", language = "plain_text")
    params += Param("icon", "Icon URL")
    params += ParametersParam("parameters", "Parameters")
  })

  registerOp(
    "Input", "black_down-pointing_triangle", OtherBoxes,
    List(), List("input"),
    new SimpleOperation(_) {
      params += Param("name", "Name")
    })

  registerOp(
    "Output", "black_up-pointing_triangle", OtherBoxes,
    List("output"), List(),
    new SimpleOperation(_) {
      params += Param("name", "Name")
    })
}
