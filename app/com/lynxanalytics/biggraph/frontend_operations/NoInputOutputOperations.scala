// Frontend operations with no input and output.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers._

class NoInputOutputOperations(env: SparkFreeEnvironment) extends OperationRegistry {
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
  val BoxDecorators = Category("Box decorators", "black", icon = "kraken")
  val AnchorBox = Category("Anchor box", "black", icon = "kraken", visible = false)

  register("Add comment", BoxDecorators)(new DecoratorOperation(_) {
    def parameters = List(
      Code("comment", "Comment", language = "plain_text")
    )
  })

  register("Anchor", BoxDecorators)(new DecoratorOperation(_) {
    def parameters = List(
      Code("description", "Description", language = "plain_text")
    // TODO: Workspace parameters.
    )
  })
}

