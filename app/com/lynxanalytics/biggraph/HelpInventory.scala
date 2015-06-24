// Writes to a file a list of identifiers that must exist in the help pages.
package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.controllers.Operations

object HelpInventory extends App {
  val ops = new Operations(null)
  val fos = new java.io.FileOutputStream(args.head)
  for (op <- ops.operationIds.map(_.toLowerCase).sorted) {
    fos.write(op.getBytes)
    fos.write('\n')
  }
  fos.close()
}
