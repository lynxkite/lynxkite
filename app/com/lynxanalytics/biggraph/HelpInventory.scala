// Writes to a file a list of identifiers that must exist in the help pages.
package com.lynxanalytics.biggraph

import java.io.FileInputStream

import com.lynxanalytics.biggraph.frontend_operations.Operations

object HelpInventory extends App {
  import scala.io.Source
  val hiddenOps = Source.fromFile(args(1)).getLines
    .map { _.toLowerCase }.toSet
  println(hiddenOps)
  val ops = new Operations(null)
  val fos = new java.io.FileOutputStream(args(0))
  for (op <- ops.operationIds.map(_.toLowerCase).sorted) {
    if (!hiddenOps.contains(op)) {
      fos.write(op.getBytes)
      fos.write('\n')
    }
  }
  fos.close()
}
