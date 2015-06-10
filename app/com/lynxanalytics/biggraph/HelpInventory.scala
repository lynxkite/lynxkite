// Prints a list of identifiers that must exist in the help pages.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.controllers.Operations

object HelpInventory extends App {
  val ops = new Operations(null)
  for (op <- ops.operationIds.sorted) {
    println(op)
  }
}
