package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

case class SparkClusterStatusResponse(
  master: String,
  workerInstances: Int)

case class SetClusterNumInstanceRequest(
  password: String,
  workerInstances: Int)

class SparkClusterController(environment: BigGraphEnvironment) {
  def getClusterStatus(request: serving.Empty): SparkClusterStatusResponse = {
    SparkClusterStatusResponse(environment.sparkContext.master, environment.numInstances)
  }

  def setClusterNumInstances(request: SetClusterNumInstanceRequest): SparkClusterStatusResponse = {
    if (request.password != "UCU8HB0d6fQJwyD8UAdDb")
      throw new IllegalArgumentException("Bad password!")
    environment.setNumInstances(request.workerInstances)
    return getClusterStatus(serving.Empty())
  }
}
