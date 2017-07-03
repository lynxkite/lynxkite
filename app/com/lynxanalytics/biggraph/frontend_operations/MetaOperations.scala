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
  val OtherBoxes = Category("Other boxes", "yellow")
  val AnchorBox = Category("Anchor box", "yellow", visible = false)

}
