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
  val OtherBoxes = Category("Other boxes", "black", icon = "kraken")
  val AnchorBox = Category("Anchor box", "black", icon = "kraken", visible = false)

  register("Add comment", OtherBoxes)(new DecoratorOperation(_) {
    override def parameters = List(
      Code("comment", "Comment", language = "plain_text")
    )
  })

  register("Anchor", AnchorBox, "anchor")(new DecoratorOperation(_) {
    override def parameters = List(
      Code("description", "Description", language = "plain_text"),
      Param("icon", "Icon URL"),
      ParametersParam("parameters", "Parameters"))
  })

  registerOp(
    "Input box", "black_down-pointing_triangle", OtherBoxes,
    List(), List("input"),
    new MinimalOperation(_) {
      override def parameters = List(Param("name", "Name"))
    })

  registerOp(
    "Output box", "black_up-pointing_triangle", OtherBoxes,
    List("output"), List(),
    new MinimalOperation(_) {
      override def parameters = List(Param("name", "Name"))
    })
}
