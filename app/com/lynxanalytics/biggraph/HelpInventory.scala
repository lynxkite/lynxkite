// Writes to a file a list of identifiers that must exist in the help pages.
package com.lynxanalytics.biggraph

import java.io.FileInputStream

import com.lynxanalytics.biggraph.frontend_operations.Operations

object HelpInventory extends App {
  val hiddenOps = Set("enhanced-example-graph")
  val ops = new Operations(null)
  val fos = new java.io.FileOutputStream(args.head)
  for (op <- ops.operationIds.map(_.toLowerCase).sorted) {
    if (!hiddenOps.contains(op)) {
      fos.write(op.getBytes("UTF-8"))
      fos.write('\n')
    }
  }
  fos.close()
}
