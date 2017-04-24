// Writes to a file a list of identifiers that must exist in the help pages.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.frontend_operations.Operations

object HelpInventory extends App {
  val hiddenOps = Set("create-enhanced-example-graph")
  val ops = new Operations(new SparkFreeEnvironment {
    def metaGraphManager = null
    def entityProgressManager = null
  })
  val fos = new java.io.FileOutputStream(args.head)
  for (op <- ops.atomicOperationIds.map(Operation.htmlID).sorted) {
    if (!hiddenOps.contains(op)) {
      fos.write(op.getBytes("UTF-8"))
      fos.write('\n')
    }
  }
  fos.close()
}
