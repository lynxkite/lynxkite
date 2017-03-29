// Frontend operations are all collected here.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.OperationRepository

class Operations(env: SparkFreeEnvironment) extends OperationRepository(env) {
  override val operations =
    new ProjectOperations(env).operations.toMap ++
      new ImportOperations(env).operations.toMap
}
